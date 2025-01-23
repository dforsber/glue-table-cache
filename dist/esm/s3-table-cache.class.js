import { BaseTableCache } from "./base-table-cache.abstract.js";
import { SqlTransformer } from "./sql-transformer.class.js";
import { getPartitionExtractor } from "./util/glue.js";
import { S3Client } from "@aws-sdk/client-s3";
import { LRUCache } from "lru-cache";
import { ETableType, } from "./types.js";
import retry from "async-retry";
import { debug } from "./util/debug.js";
const log = debug("s3-table-cache");
const logAws = debug("s3-table-cache:aws");
const S3_PREFIX = "s3";
const defaultConfig = {
    region: "eu-west-1",
    maxEntries: 100,
    tableMetadataTtlMs: 3600000, // 1 hour
    s3ListingRefresTtlhMs: 3600000, // 1 hour
};
export class S3TableCache extends BaseTableCache {
    s3Client;
    tableCache;
    sqlTransformer;
    constructor(config) {
        const fullConfig = {
            ...defaultConfig,
            ...config,
            region: config?.region || process.env.AWS_REGION || defaultConfig.region || "eu-west-1",
        };
        super(fullConfig);
        log("Initialised GlueTableCache config:", JSON.stringify(this.config));
        const awsSdkParams = {
            region: this.config.region,
            credentials: this.config.credentials,
        };
        this.s3Client = new S3Client(awsSdkParams);
        // Initialize metadata cache
        this.tableCache = new LRUCache({
            max: this.config.maxEntries ?? 100,
            ttl: config?.tableMetadataTtlMs ?? 3600000,
        });
    }
    clearCache() {
        this.tableCache.clear();
        return super.clearCache();
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
                    // TODO: ...
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
    async convertQuery(query) {
        if (!this.db)
            await this.__connect();
        if (!this.db)
            throw new Error("DB not connected");
        if (!this.sqlTransformer)
            this.sqlTransformer = new SqlTransformer(this.db, S3_PREFIX);
        if (!this.sqlTransformer)
            throw new Error("SQL transformer not initialized");
        const setupSql = await this.getViewSetupSql(query);
        const transformedQuery = await this.sqlTransformer.transformTableQuery(query);
        return setupSql.join("") + transformedQuery;
    }
    async getViewSetupSql(query) {
        if (!this.db)
            await this.__connect();
        if (!this.db)
            throw new Error("DB not connected");
        if (!this.sqlTransformer)
            this.sqlTransformer = new SqlTransformer(this.db, S3_PREFIX);
        if (!this.sqlTransformer)
            throw new Error("SQL transformer not initialized");
        // Generate all SQL statements needed
        const statements = [];
        // Get Glue Table references from the query
        const tableRefs = await this.sqlTransformer.getQueryTableRefs(query);
        log("Found Glue Table references: %O", tableRefs);
        await Promise.all(tableRefs.map(async ({ database, table }) => {
            log("Found Glue Table reference: %s", { database, table });
            const metadata = await this.getTableMetadataCached(database, table);
            if (!metadata)
                throw new Error("Metadata not found");
            const tblName = `${database}_${table}`;
            // TODO: ..
            let partitionKeys = (metadata.table.PartitionKeys || []).map((k) => k.Name);
            const files = [];
            switch (metadata.tableType) {
                default:
                case ETableType.S3_TABLE: {
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
            const viewSqls = await this.sqlTransformer.getTableViewSql(query, files.length);
            statements.push(...viewSqls);
        }));
        const trimmed = statements.map((stmt) => stmt.trim());
        log(trimmed);
        return trimmed;
    }
}
