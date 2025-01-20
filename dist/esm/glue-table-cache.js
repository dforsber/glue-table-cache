import { GlueClient } from "@aws-sdk/client-glue";
import { S3Client } from "@aws-sdk/client-s3";
import { LRUCache } from "lru-cache";
import { DuckDBInstance } from "@duckdb/node-api";
import debug from "debug";
import { SqlTransformer } from "./sql-transformer.js";
import { ETableType, } from "./types.js";
import { listS3Objects, mapS3PathsToInfo } from "./util/s3.js";
import { getGlueTableMetadata, getPartitionExtractor } from "./util/glue.js";
import { getIcebergS3FilesStmts } from "./util/iceberg.js";
import { Mutex } from "async-mutex";
import retry from "async-retry";
const log = debug("glue-table-cache");
const logAws = debug("glue-table-cache:aws");
const defaultConfig = {
    region: "eu-west-1",
    maxEntries: 100,
    glueTableMetadataTtlMs: 3600000, // 1 hour
    s3ListingRefresTtlhMs: 3600000, // 1 hour
    proxyAddress: undefined,
};
export class GlueTableCache {
    tableCache;
    s3ListingCache;
    glueClient;
    s3Client;
    db;
    sqlTransformer;
    config;
    constructor(config = defaultConfig) {
        log("GlueTableCache config:", JSON.stringify(config));
        this.config = {
            ...defaultConfig,
            ...config,
            region: config?.region || process.env.AWS_REGION || defaultConfig.region || "eu-west-1",
        };
        if (this.config.proxyAddress) {
            try {
                new URL(this.config.proxyAddress);
                if (!this.config.proxyAddress.endsWith("/")) {
                    this.config.proxyAddress = this.config.proxyAddress + "/";
                }
                log("Using proxyAddress:", this.config.proxyAddress);
            }
            catch (err) {
                console.error(err);
                this.config.proxyAddress = undefined;
            }
        }
        log("Initialised GlueTableCache config:", JSON.stringify(config));
        const awsSdkParams = {
            region: config.region,
            credentials: this.config.credentials,
        };
        this.glueClient = new GlueClient(awsSdkParams);
        this.s3Client = new S3Client(awsSdkParams);
        // Initialize metadata cache
        this.tableCache = new LRUCache({
            max: this.config.maxEntries,
            ttl: this.config.glueTableMetadataTtlMs,
        });
        // Initialize S3 listing cache
        this.s3ListingCache = new LRUCache({
            max: this.config.maxEntries,
            ttl: this.config.s3ListingRefresTtlhMs,
        });
    }
    setCredentials(credentials) {
        this.config.credentials = credentials;
    }
    // because constructor can't be async and we don't want the clients to worry about this
    async __connect() {
        if (!this.db)
            this.db = await (await DuckDBInstance.create(":memory:")).connect();
        if (!this.db)
            throw new Error("Could not create DuckDB instance (neo)");
        if (this.config.credentials?.accessKeyId && this.config.credentials?.secretAccessKey) {
            const { accessKeyId, secretAccessKey, sessionToken } = this.config.credentials;
            log("Using configured credentials for DuckDB (neo)");
            await this.__runAndReadAll(
            // white space is ok...
            `CREATE SECRET s3SecretForIcebergFromCreds (
            TYPE S3,
            KEY_ID '${accessKeyId}',
            SECRET '${secretAccessKey}',
            ${sessionToken ? `SESSION_TOKEN \'${sessionToken}\',` : ""}
            REGION '${this.config.region}'
        );`);
        }
        else {
            log("Using default credentials chain provider for DuckDB (neo)");
            await this.__runAndReadAll(`CREATE OR REPLACE SECRET s3SecretForIcebergWithProvider ( TYPE S3, PROVIDER CREDENTIAL_CHAIN );`);
        }
        if (!this.sqlTransformer)
            this.sqlTransformer = new SqlTransformer(this.db);
        return this.db;
    }
    clearCache() {
        this.tableCache.clear();
        this.s3ListingCache.clear();
    }
    close() {
        this.db?.close();
        this.db = undefined;
        this.sqlTransformer = undefined;
    }
    getCacheKeyWithMutex(cache, key) {
        // NOTE: We don't need to use Mutex here as nodejs event loop is single threaded and this function is synchronous
        let cached = cache.get(key);
        if (!cached) {
            cache.set(key, {
                mutex: new Mutex(),
                timestamp: Date.now(),
                data: undefined,
            });
            cached = cache.get(key);
        }
        if (!cached)
            throw new Error("ummm");
        return cached;
    }
    async getTableMetadataCached(database, tableName) {
        const key = `${database}_${tableName}`;
        log("Getting table metadata for %s", key);
        const cached = this.getCacheKeyWithMutex(this.tableCache, key);
        if (cached.error)
            delete cached.error; // reset errors, if any
        if (!cached || !cached.mutex)
            throw new Error("Failed to init cache entry");
        if (!cached.data) {
            return cached.mutex.runExclusive(async () => retry(async (bail, _attempt) => {
                if (cached.data)
                    return cached.data; // already filled up for this key by some other concurrent request
                if (cached.error) {
                    bail(cached.error); // queued requests should throw too.
                    return;
                }
                log("Cache miss for %s, refreshing...", key);
                try {
                    logAws("Fetching table metadata from AWS for %s.%s", database, tableName);
                    const metadata = await getGlueTableMetadata(this.glueClient, database, tableName);
                    const now = Date.now();
                    cached.timestamp = now;
                    cached.data = metadata;
                    return cached.data;
                    // eslint-disable-next-line @typescript-eslint/no-explicit-any
                }
                catch (error) {
                    log("getTableMetadataCached ERROR:", error?.$metadata);
                    if (error?.$metadata?.httpStatusCode === 403 ||
                        error?.[0]?.$metadata?.httpStatusCode === 403 ||
                        error?.$metadata?.httpStatusCode === 400 ||
                        error?.[0]?.$metadata?.httpStatusCode === 400 ||
                        error?.message.includes("HTTP 40")) {
                        bail(error);
                        return;
                    }
                    throw error;
                }
            }, {
                retries: 3,
                minTimeout: 200,
                maxTimeout: 500,
                onRetry: (e, a) => log("getTableMetadataCached -- retry:", a, e),
            }));
        }
        else {
            log("Cache hit for %s", key);
            return cached.data;
        }
    }
    invalidateTable(database, tableName) {
        const key = `${database}_${tableName}`;
        this.tableCache.delete(key);
        // Also invalidate any S3 listings for this table
        for (const cacheKey of this.s3ListingCache.keys()) {
            if (cacheKey.includes(key)) {
                this.s3ListingCache.delete(cacheKey);
            }
        }
    }
    async createGlueTableFilesVarSql(database, tableName, filters) {
        if (!this.db)
            await this.__connect();
        if (!this.db)
            throw new Error("DB not connected");
        if (!this.sqlTransformer)
            this.sqlTransformer = new SqlTransformer(this.db);
        if (!this.sqlTransformer)
            throw new Error("SQL transformer not initialized");
        const tblName = `${database}_${tableName}`;
        let query = `SELECT path FROM "${tblName}_s3_listing"`;
        if (filters && filters.length > 0) {
            query += ` WHERE ${filters.join(" AND ")}`;
        }
        // Create a list of paths as a string array
        const safeVarName = this.sqlTransformer?.getGlueTableFilesVarName(database, tableName);
        if (this.config.proxyAddress) {
            // For example: s3:// --> https://locahost:3203/
            return `SET VARIABLE ${safeVarName} = ( SELECT list(replace(path, 's3://', '${this.config.proxyAddress}')) FROM (${query}));`;
        }
        return `SET VARIABLE ${safeVarName} = ( SELECT list(path) FROM (${query}));`;
    }
    async convertGlueTableQuery(query) {
        if (!this.db)
            await this.__connect();
        if (!this.db)
            throw new Error("DB not connected");
        if (!this.sqlTransformer)
            this.sqlTransformer = new SqlTransformer(this.db);
        if (!this.sqlTransformer)
            throw new Error("SQL transformer not initialized");
        const setupSql = await this.getGlueTableViewSetupSql(query);
        const transformedQuery = await this.sqlTransformer.transformGlueTableQuery(query);
        return setupSql.join("") + transformedQuery;
    }
    async getGlueTableViewSetupSql(query) {
        if (!this.db)
            await this.__connect();
        if (!this.db)
            throw new Error("DB not connected");
        if (!this.sqlTransformer)
            this.sqlTransformer = new SqlTransformer(this.db);
        if (!this.sqlTransformer)
            throw new Error("SQL transformer not initialized");
        // Generate all SQL statements needed
        const statements = [];
        // Get Glue Table references from the query
        const tableRefs = await this.sqlTransformer.getQueryGlueTableRefs(query);
        log("Found Glue Table references: %O", tableRefs);
        await Promise.all(tableRefs.map(async ({ database, table }) => {
            log("Found Glue Table reference: %s", { database, table });
            const metadata = await this.getTableMetadataCached(database, table);
            if (!metadata)
                throw new Error("Metadata not found");
            const tblName = `${database}_${table}`;
            const baseLocation = metadata.table.StorageDescriptor?.Location;
            if (!baseLocation) {
                throw new Error(`No storage location found for ${tblName}`);
            }
            let partitionKeys = (metadata.table.PartitionKeys || []).map((k) => k.Name);
            const files = [];
            switch (metadata.tableType) {
                case ETableType.ICEBERG: {
                    const res = await this.__listS3IcebergFilesCached(baseLocation, partitionKeys);
                    if (res)
                        files.push(...res);
                    break;
                }
                default: {
                    const res = await this.__listS3FilesCached(baseLocation, partitionKeys);
                    if (res)
                        files.push(...res);
                    break;
                }
            }
            // 1. Create base table for file paths
            statements.push(`CREATE OR REPLACE TABLE "${tblName}_s3_files" AS ` +
                `SELECT path FROM (VALUES ${files.length ? files.map((f) => `('${f.path}')`).join(",") : "( '' )"}) t(path);`);
            // 2. Create listing table with partition columns
            const extractors = await Promise.all(partitionKeys.map(async (k) => `${await getPartitionExtractor(k, metadata)} as ${k}`));
            statements.push(`CREATE OR REPLACE TABLE "${tblName}_s3_listing" AS ` +
                `SELECT path, ${extractors.join(", ")} FROM "${tblName}_s3_files";`);
            // 3. Create indexes on partition columns
            for (const key of partitionKeys) {
                statements.push(`CREATE INDEX IF NOT EXISTS idx_${key} ON "${tblName}_s3_listing" (${key});`);
            }
            // 4. Extract partition filters from the query and set the variable with filtered file list
            if (!this.sqlTransformer)
                throw new Error("SQL transformer not initialized"); // make TS happy
            partitionKeys = (metadata.table.PartitionKeys || []).map((k) => k.Name);
            const partitionFilters = await this.sqlTransformer.extractPartitionFilters(query, partitionKeys);
            // 5. Query specific partition pruned SQL VARIABLE
            let variableQuery = `SELECT list(path) FROM "${tblName}_s3_listing"`;
            if (this.config.proxyAddress) {
                // For example: s3:// --> https://locahost:3203/
                variableQuery = `SELECT list(replace(path, 's3://', '${this.config.proxyAddress}')) FROM "${tblName}_s3_listing"`;
            }
            if (partitionFilters.length > 0) {
                variableQuery += ` WHERE ${partitionFilters.join(" AND ")}`;
            }
            const queryVarName = this.sqlTransformer.getQueryFilesVarName(database, table);
            statements.push(`SET VARIABLE ${queryVarName} = (${variableQuery});`);
            // 6. Unfiltered Glue Table VIEW
            const glueTableViewSql = await this.createGlueTableFilesVarSql(database, table);
            if (glueTableViewSql)
                statements.push(glueTableViewSql);
            const viewSqls = await this.sqlTransformer.getGlueTableViewSql(query, files.length);
            statements.push(...viewSqls);
        }));
        const trimmed = statements.map((stmt) => stmt.trim());
        log(trimmed);
        return trimmed;
    }
    async __listS3IcebergFilesCached(s3Path, partitionKeys) {
        const key = `${s3Path}:${partitionKeys.join(",")}`;
        const cached = this.getCacheKeyWithMutex(this.s3ListingCache, key);
        if (!cached)
            throw new Error("Could not initialise cache entry");
        if (cached.error)
            delete cached.error; // reset errors, if any
        if (cached.data) {
            logAws("Using cached S3 (Iceberg) listing for %s", s3Path);
            return cached.data;
        }
        else {
            return cached.mutex.runExclusive(async () => retry(async (bail, attempt) => {
                if (cached.data)
                    return cached.data; // another concurrent call, filled up the cache already
                if (cached.error) {
                    bail(cached.error); // queued requests should throw too.
                    return;
                }
                try {
                    // For Iceberg Table, fetch the latest version files
                    const sql = getIcebergS3FilesStmts(s3Path);
                    logAws(sql);
                    if (!this.db)
                        await this.__connect();
                    if (!this.db)
                        throw new Error("Could not create db connection");
                    const last = sql.pop();
                    if (!last)
                        throw new Error("No SQL statements generated");
                    for await (const stmt of sql) {
                        log(stmt);
                        await this.db.runAndReadAll(stmt);
                    }
                    const res = (await this.db.runAndReadAll(last)).getRows();
                    const listing = mapS3PathsToInfo(res.flat(), partitionKeys);
                    logAws("Listing Iceberg S3 files for %s", { s3Path, partitionKeys, listing });
                    this.s3ListingCache.set(key, { ...cached, timestamp: Date.now(), data: listing });
                    cached.error = undefined;
                    return listing;
                    // eslint-disable-next-line @typescript-eslint/no-explicit-any
                }
                catch (error) {
                    cached.error = error;
                    log("__listS3IcebergFilesCached ERROR:", attempt, error);
                    if (error?.$metadata?.httpStatusCode === 403 ||
                        error?.[0]?.$metadata?.httpStatusCode === 403 ||
                        error?.$metadata?.httpStatusCode === 400 ||
                        error?.[0]?.$metadata?.httpStatusCode === 400 ||
                        error?.message.includes("HTTP 40")) {
                        bail(error);
                        return;
                    }
                    throw error;
                }
            }, {
                retries: 3,
                minTimeout: 200,
                maxTimeout: 500,
                onRetry: (e, a) => log("__listS3IcebergFilesCached -- retry:", a, e),
            }));
        }
    }
    async __listS3FilesCached(s3Path, partitionKeys) {
        const key = `${s3Path}:${partitionKeys.join(",")}`;
        const cached = this.getCacheKeyWithMutex(this.s3ListingCache, key);
        if (!cached)
            throw new Error("Could not initialise cache entry");
        if (cached.error)
            delete cached.error; // reset errors, if any
        if (cached.data) {
            logAws("Using cached S3 listing for %s", s3Path);
            return cached.data;
        }
        else {
            return cached.mutex.runExclusive(async () => retry(async (bail, attempt) => {
                if (cached.data)
                    return cached.data; // another concurrent call, filled up the cache already
                if (cached.error) {
                    bail(cached.error); // queued requests should throw too.
                    return;
                }
                try {
                    logAws("Listing S3 files for %s", s3Path);
                    const files = await listS3Objects(this.s3Client, s3Path, partitionKeys);
                    this.s3ListingCache.set(key, { ...cached, timestamp: Date.now(), data: files });
                    return files;
                    // eslint-disable-next-line @typescript-eslint/no-explicit-any
                }
                catch (error) {
                    log("__listS3FilesCached ERROR:", attempt, error);
                    if (error?.$metadata?.httpStatusCode === 403 ||
                        error?.[0]?.$metadata?.httpStatusCode === 403 ||
                        error?.$metadata?.httpStatusCode === 400 ||
                        error?.[0]?.$metadata?.httpStatusCode === 400 ||
                        error?.message.includes("HTTP 40")) {
                        bail(error);
                        return;
                    }
                    throw error;
                }
            }, {
                retries: 3,
                minTimeout: 200,
                maxTimeout: 500,
                onRetry: (e, a) => log("__listS3FilesCached -- retry:", a, e),
            }));
        }
    }
    // tests use this
    async __runAndReadAll(query) {
        if (!this.db)
            await this.__connect();
        if (!this.db)
            throw new Error("DB not connected");
        return this.db.runAndReadAll(query);
    }
}
