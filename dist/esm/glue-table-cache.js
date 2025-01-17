import { GetPartitionsCommand, GetTableCommand, GlueClient, } from "@aws-sdk/client-glue";
import { S3Client, ListObjectsV2Command } from "@aws-sdk/client-s3";
import { LRUCache } from "lru-cache";
import { DuckDBInstance } from "@duckdb/node-api";
import debug from "debug";
import { SqlTransformer } from "./sql-transformer.js";
const log = debug("glue-table-cache");
const logAws = debug("glue-table-cache:aws");
const defaultConfig = {
    region: "eu-west-1",
    maxEntries: 100,
    forceRefreshOnError: true,
    glueTableMetadataTtlMs: 3600000, // 1 hour
    s3ListingRefreshMs: 3600000, // 1 hour
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
        log("Initializing GlueTableCache for region %s with config %O", config);
        this.config = {
            ...defaultConfig,
            ...config,
            region: config?.region || process.env.AWS_REGION || defaultConfig.region,
        };
        this.glueClient = new GlueClient({ region: config.region });
        this.s3Client = new S3Client({ region: config.region });
        // Initialize metadata cache
        this.tableCache = new LRUCache({
            max: this.config.maxEntries,
            ttl: this.config.glueTableMetadataTtlMs,
        });
        // Initialize S3 listing cache
        this.s3ListingCache = new LRUCache({
            max: this.config.maxEntries,
            ttl: this.config.s3ListingRefreshMs,
        });
    }
    // tests use this
    async runAndReadAll(query) {
        if (!this.db)
            await this.connect();
        if (!this.db)
            throw new Error("DB not connected");
        return this.db.runAndReadAll(query);
    }
    async connect() {
        if (!this.db)
            this.db = await (await DuckDBInstance.create(":memory:")).connect();
        if (!this.sqlTransformer)
            this.sqlTransformer = new SqlTransformer(this.db);
        return this.db;
    }
    close() {
        this.db?.close();
        this.db = undefined;
        this.sqlTransformer = undefined;
    }
    async getTableMetadata(database, tableName) {
        const key = `${database}_${tableName}`;
        log("Getting table metadata for %s", key);
        const cached = this.tableCache.get(key);
        if (!cached) {
            log("Cache miss for %s, refreshing...", key);
            const metadata = await this.refreshTableMetadata(database, tableName);
            const now = Date.now();
            const entry = {
                timestamp: now,
                data: metadata,
            };
            this.tableCache.set(key, entry);
            return metadata;
        }
        log("Cache hit for %s", key);
        return cached.data;
    }
    async refreshTableMetadata(database, tableName) {
        try {
            logAws("Fetching table metadata from AWS for %s.%s", database, tableName);
            const tableRequest = { DatabaseName: database, Name: tableName };
            const tableResponse = await this.glueClient.send(new GetTableCommand(tableRequest));
            const table = tableResponse.Table;
            if (!table)
                throw new Error(`Table ${database}.${tableName} not found`);
            const metadata = { timestamp: Date.now(), table: table };
            // Handle partition projection if enabled
            if (table.Parameters?.["projection.enabled"] === "true") {
                metadata.projectionPatterns = this.parseProjectionPatterns(table.Parameters);
            }
            else if (table.PartitionKeys && table.PartitionKeys.length > 0) {
                // Load partition metadata for standard partitioned tables
                metadata.partitionMetadata = await this.loadPartitionMetadata(database, tableName);
            }
            return metadata;
        }
        catch (error) {
            if (this.config.forceRefreshOnError)
                this.tableCache.delete(`${database}_${tableName}`);
            throw error;
        }
    }
    parseProjectionValue(property, value) {
        switch (property) {
            case "type":
                return value;
            case "format":
                return value;
            case "range":
                // Handle both JSON array format and comma-separated format
                try {
                    return JSON.parse(value);
                }
                catch {
                    return value.split(",").map((v) => v.trim());
                }
            case "values":
                return JSON.parse(value);
            default:
                return value;
        }
    }
    parseProjectionPatterns(parameters) {
        const patterns = {};
        Object.entries(parameters)
            .filter(([key]) => key.startsWith("projection."))
            .forEach(([key, value]) => {
            const match = key.match(/projection\.(\w+)\.(type|range|format|values)/);
            if (match) {
                const [_, column, property] = match;
                if (!patterns[column]) {
                    patterns[column] = { type: "enum" };
                }
                if (property === "type" ||
                    property === "format" ||
                    property === "range" ||
                    property === "values") {
                    patterns[column][property] = this.parseProjectionValue(property, value);
                }
            }
        });
        return {
            enabled: true,
            patterns,
        };
    }
    async loadPartitionMetadata(database, tableName) {
        // Implementation for loading standard partition metadata
        const command = new GetPartitionsCommand({
            DatabaseName: database,
            TableName: tableName,
            // Add pagination handling for large partition sets
        });
        try {
            const response = await this.glueClient.send(command);
            if (!response.Partitions || response.Partitions.length === 0) {
                return { keys: [], values: [] };
            }
            return {
                keys: response.Partitions[0].Values || [],
                values: response.Partitions.map((p) => ({
                    values: p.Values || [],
                    location: p.StorageDescriptor?.Location,
                })) || [],
            };
        }
        catch (error) {
            console.warn(`Failed to load partitions for ${database}_${tableName}:`, error);
            return { keys: [], values: [] };
        }
    }
    // Utility methods for cache management
    clearCache() {
        this.tableCache.clear();
        this.s3ListingCache.clear();
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
    parseS3Path(s3Path) {
        const url = new URL(s3Path);
        return {
            bucket: url.hostname,
            prefix: url.pathname.substring(1), // remove leading '/'
        };
    }
    extractPartitionValues(path, partitionKeys) {
        const values = {};
        for (const key of partitionKeys) {
            const match = path.match(new RegExp(`${key}=([^/]+)`));
            if (match)
                values[key] = match[1];
        }
        return values;
    }
    async listS3Files(s3Path, partitionKeys) {
        const cacheKey = `${s3Path}:${partitionKeys.join(",")}`;
        const cached = this.s3ListingCache.get(cacheKey);
        if (cached) {
            logAws("Using cached S3 listing for %s", s3Path);
            return cached.data;
        }
        logAws("Listing S3 files for %s", s3Path);
        const { bucket, prefix } = this.parseS3Path(s3Path);
        const files = [];
        // Ensure prefix ends with "/"
        const normalizedPrefix = prefix.endsWith("/") ? prefix : `${prefix}/`;
        let continuationToken;
        do {
            const command = new ListObjectsV2Command({
                Bucket: bucket,
                Prefix: normalizedPrefix,
                ContinuationToken: continuationToken,
                MaxKeys: 1000,
            });
            const response = await this.s3Client.send(command);
            if (response.Contents) {
                for (const object of response.Contents) {
                    if (object.Key && !object.Key.includes("_$folder$")) {
                        const path = `s3://${bucket}/${object.Key}`;
                        const partitionValues = this.extractPartitionValues(path, partitionKeys);
                        files.push({ path, partitionValues });
                    }
                }
            }
            continuationToken = response.NextContinuationToken;
        } while (continuationToken);
        // Cache the results
        const now = Date.now();
        this.s3ListingCache.set(cacheKey, { timestamp: now, data: files });
        return files;
    }
    async getPartitionExtractor(key, metadata) {
        // Check if this is a projection-enabled table
        if (metadata.projectionPatterns?.enabled) {
            const pattern = metadata.projectionPatterns.patterns[key];
            if (!pattern) {
                throw new Error(`No projection pattern found for partition key ${key}`);
            }
            // Handle different projection types
            switch (pattern.type) {
                case "date":
                    // For date projections, use the format pattern to build regex
                    const dateFormat = pattern.format || "yyyy-MM-dd";
                    const dateRegex = this.convertDateFormatToRegex(dateFormat);
                    return `regexp_extract(path, '(${dateRegex})', 1)`;
                case "integer":
                    // For integer projections, extract full numeric values
                    return "CAST(regexp_extract(path, '/([0-9]+)/', 1) AS INTEGER)";
                case "enum":
                    // For enum projections, extract the last path component before the filename
                    return "regexp_extract(path, '/([^/]+)/[^/]*$', 1)";
                case "injected":
                    // For injected values, extract them from the SQL query filters
                    // The query must contain static equality conditions
                    if (!this.sqlTransformer)
                        await this.connect();
                    if (!this.sqlTransformer)
                        throw new Error("SQL transformer not initialized");
                    throw new Error("Injected partition values not supported yet");
                default:
                    throw new Error(`Unsupported projection type: ${pattern.type}`);
            }
        }
        // Default to Hive-style partitioning
        return `regexp_extract(path, '${key}=([^/]+)', 1)`;
    }
    convertDateFormatToRegex(format) {
        // Convert Java SimpleDateFormat patterns to regex patterns
        const conversions = {
            yyyy: "\\d{4}",
            MM: "\\d{2}",
            dd: "\\d{2}",
            HH: "\\d{2}",
            mm: "\\d{2}",
            ss: "\\d{2}",
        };
        let regex = format;
        for (const [pattern, replacement] of Object.entries(conversions)) {
            regex = regex.replace(pattern, replacement);
        }
        return regex;
    }
    async createGlueTableFilesVarSql(database, tableName, filters) {
        if (!this.db)
            await this.connect();
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
        return `SET VARIABLE ${safeVarName} = ( SELECT list(path) FROM (${query}));`;
    }
    async convertGlueTableQuery(query) {
        if (!this.db)
            await this.connect();
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
            await this.connect();
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
            const metadata = await this.getTableMetadata(database, table);
            const tblName = `${database}_${table}`;
            const baseLocation = metadata.table.StorageDescriptor?.Location;
            if (!baseLocation) {
                throw new Error(`No storage location found for ${tblName}`);
            }
            let partitionKeys = (metadata.table.PartitionKeys || []).map((k) => k.Name);
            const files = await this.listS3Files(baseLocation, partitionKeys);
            // 1. Create base table for file paths
            statements.push(`CREATE OR REPLACE TABLE "${tblName}_s3_files" AS ` +
                `SELECT path FROM (VALUES ${files.length ? files.map((f) => `('${f.path}')`).join(",") : "( '' )"}) t(path);`);
            // 2. Create listing table with partition columns
            const extractors = await Promise.all(partitionKeys.map(async (k) => `${await this.getPartitionExtractor(k, metadata)} as ${k}`));
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
}
