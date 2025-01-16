import {
  GetPartitionsCommand,
  GetTableCommand,
  GetTableRequest,
  GlueClient,
} from "@aws-sdk/client-glue";
import { S3Client, ListObjectsV2Command } from "@aws-sdk/client-s3";
import { LRUCache } from "lru-cache";
import { DuckDBInstance, DuckDBConnection } from "@duckdb/node-api";
import debug from "debug";
import { SqlTransformer } from "./sql-transformer.js";
import {
  type CachedTableMetadata,
  type CacheConfig,
  type CacheEntry,
  type ProjectionPattern,
  type S3FileInfo,
} from "./types.js";
import { DuckDBResultReader } from "@duckdb/node-api/lib/DuckDBResultReader.js";

export type { S3FileInfo };

const log = debug("glue-table-cache");
const logAws = debug("glue-table-cache:aws");

interface PartitionFilter {
  sql: string;
  parameters?: unknown[];
}

const defaultConfig = {
  ttlMs: 3600000, // 1 hour
  maxEntries: 100,
  forceRefreshOnError: true,
  s3ListingRefreshMs: 300000, // 5 minutes
};

export class GlueTableCache {
  private tableCache: LRUCache<string, CacheEntry<CachedTableMetadata>>;
  private s3ListingCache: LRUCache<string, CacheEntry<S3FileInfo[]>>;
  private glueClient: GlueClient;
  private s3Client: S3Client;
  private db: DuckDBConnection | undefined;
  private sqlTransformer: SqlTransformer | undefined;
  private config: CacheConfig;

  constructor(region: string, config: Partial<CacheConfig> = defaultConfig) {
    log("Initializing GlueTableCache for region %s with config %O", region, config);
    this.glueClient = new GlueClient({ region });
    this.s3Client = new S3Client({ region });
    this.config = { ...defaultConfig, ...config };

    // Initialize metadata cache
    this.tableCache = new LRUCache({
      max: this.config.maxEntries,
      ttl: this.config.ttlMs,
    });

    // Initialize S3 listing cache
    this.s3ListingCache = new LRUCache({
      max: this.config.maxEntries,
      ttl: this.config.s3ListingRefreshMs,
    });
  }

  // tests use this
  private async runAndReadAll(query: string): Promise<DuckDBResultReader> {
    if (!this.db) await this.connect();
    if (!this.db) throw new Error("DB not connected");
    return this.db.runAndReadAll(query);
  }

  private async connect(): Promise<DuckDBConnection> {
    if (!this.db) this.db = await (await DuckDBInstance.create(":memory:")).connect();
    if (!this.sqlTransformer) this.sqlTransformer = new SqlTransformer(this.db);
    return this.db;
  }

  close(): void {
    this.db?.close();
    this.db = undefined;
  }

  async getTableMetadata(database: string, tableName: string): Promise<CachedTableMetadata> {
    const key = `${database}.${tableName}`;
    log("Getting table metadata for %s", key);
    const cached = this.tableCache.get(key);

    if (!cached) {
      log("Cache miss for %s, refreshing...", key);
      const metadata = await this.refreshTableMetadata(database, tableName);
      const now = Date.now();
      const entry: CacheEntry<CachedTableMetadata> = {
        timestamp: now,
        data: metadata,
      };
      this.tableCache.set(key, entry);
      return metadata;
    }

    log("Cache hit for %s", key);
    return cached.data;
  }

  private async refreshTableMetadata(
    database: string,
    tableName: string
  ): Promise<CachedTableMetadata> {
    logAws("Fetching table metadata from AWS for %s.%s", database, tableName);
    const tableRequest: GetTableRequest = {
      DatabaseName: database,
      Name: tableName,
    };

    try {
      const tableResponse = await this.glueClient.send(new GetTableCommand(tableRequest));
      const table = tableResponse.Table;

      if (!table) {
        throw new Error(`Table ${database}.${tableName} not found`);
      }

      const metadata: CachedTableMetadata = {
        timestamp: Date.now(),
        table: table,
      };

      // Handle partition projection if enabled
      if (table.Parameters?.["projection.enabled"] === "true") {
        metadata.projectionPatterns = this.parseProjectionPatterns(table.Parameters);
      } else if (table.PartitionKeys && table.PartitionKeys.length > 0) {
        // Load partition metadata for standard partitioned tables
        metadata.partitionMetadata = await this.loadPartitionMetadata(database, tableName);
      }

      return metadata;
    } catch (error) {
      if (this.config.forceRefreshOnError) {
        this.tableCache.delete(`${database}.${tableName}`);
      }
      throw error;
    }
  }

  private parseProjectionValue(property: string, value: string): string | string[] | number[] {
    switch (property) {
      case "type":
        return value as "enum" | "integer" | "date" | "injected";
      case "format":
        return value;
      case "range":
        // Handle both JSON array format and comma-separated format
        try {
          return JSON.parse(value);
        } catch {
          return value.split(",").map((v) => v.trim());
        }
      case "values":
        return JSON.parse(value);
      default:
        return value;
    }
  }

  private parseProjectionPatterns(parameters: Record<string, string>): {
    enabled: boolean;
    patterns: Record<string, ProjectionPattern>;
  } {
    const patterns: Record<string, ProjectionPattern> = {};

    Object.entries(parameters)
      .filter(([key]) => key.startsWith("projection."))
      .forEach(([key, value]) => {
        const match = key.match(/projection\.(\w+)\.(type|range|format|values)/);
        if (match) {
          const [_, column, property] = match;
          if (!patterns[column]) {
            patterns[column] = { type: "enum" };
          }
          if (
            property === "type" ||
            property === "format" ||
            property === "range" ||
            property === "values"
          ) {
            (patterns[column] as ProjectionPattern as unknown as Record<string, unknown>)[
              property
            ] = this.parseProjectionValue(property, value);
          }
        }
      });

    return {
      enabled: true,
      patterns,
    };
  }

  private async loadPartitionMetadata(database: string, tableName: string) {
    // Implementation for loading standard partition metadata
    const command = new GetPartitionsCommand({
      DatabaseName: database,
      TableName: tableName,
      // Add pagination handling for large partition sets
    });

    try {
      const response = await this.glueClient.send(command);
      if (!response.Partitions || response.Partitions.length === 0) {
        return {
          keys: [],
          values: [],
        };
      }
      return {
        keys: response.Partitions[0].Values || [],
        values:
          response.Partitions.map((p) => ({
            values: p.Values || [],
            location: p.StorageDescriptor?.Location,
          })) || [],
      };
    } catch (error) {
      console.warn(`Failed to load partitions for ${database}.${tableName}:`, error);
      return {
        keys: [],
        values: [],
      };
    }
  }

  // Utility methods for cache management
  clearCache(): void {
    this.tableCache.clear();
    this.s3ListingCache.clear();
  }

  invalidateTable(database: string, tableName: string): void {
    const key = `${database}.${tableName}`;
    this.tableCache.delete(key);
    // Also invalidate any S3 listings for this table
    for (const cacheKey of this.s3ListingCache.keys()) {
      if (cacheKey.includes(key)) {
        this.s3ListingCache.delete(cacheKey);
      }
    }
  }

  private parseS3Path(s3Path: string): { bucket: string; prefix: string } {
    const url = new URL(s3Path);
    return {
      bucket: url.hostname,
      prefix: url.pathname.substring(1), // remove leading '/'
    };
  }

  private extractPartitionValues(path: string, partitionKeys: string[]): Record<string, string> {
    const values: Record<string, string> = {};
    for (const key of partitionKeys) {
      const match = path.match(new RegExp(`${key}=([^/]+)`));
      if (match) {
        values[key] = match[1];
      }
    }
    return values;
  }

  private async listS3Files(s3Path: string, partitionKeys: string[]): Promise<S3FileInfo[]> {
    const cacheKey = `${s3Path}:${partitionKeys.join(",")}`;
    const cached = this.s3ListingCache.get(cacheKey);

    if (cached) {
      logAws("Using cached S3 listing for %s", s3Path);
      return cached.data;
    }

    logAws("Listing S3 files for %s", s3Path);
    const { bucket, prefix } = this.parseS3Path(s3Path);
    const files: S3FileInfo[] = [];

    // Ensure prefix ends with "/"
    const normalizedPrefix = prefix.endsWith("/") ? prefix : `${prefix}/`;

    let continuationToken: string | undefined;
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
    this.s3ListingCache.set(cacheKey, {
      timestamp: now,
      data: files,
    });

    return files;
  }

  private async getPartitionExtractor(key: string, metadata: CachedTableMetadata): Promise<string> {
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
          if (!this.sqlTransformer) await this.connect();
          if (!this.sqlTransformer) throw new Error("SQL transformer not initialized");
          throw new Error("Injected partition values not supported yet");

        default:
          throw new Error(`Unsupported projection type: ${pattern.type}`);
      }
    }

    // Default to Hive-style partitioning
    return `regexp_extract(path, '${key}=([^/]+)', 1)`;
  }

  private convertDateFormatToRegex(format: string): string {
    // Convert Java SimpleDateFormat patterns to regex patterns
    const conversions: Record<string, string> = {
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

  private async ensureS3ListingTable(
    database: string,
    tableName: string,
    metadata: CachedTableMetadata
  ): Promise<void> {
    if (!this.db) await this.connect();
    if (!this.db) throw new Error("DB not connected");
    const tblName = `${database}.${tableName}`;
    const cached = this.s3ListingCache.get(tblName);
    const now = Date.now();

    if (cached && now - cached.timestamp < this.config.s3ListingRefreshMs) {
      log("Using cached S3 listing for %s", tblName);
      return;
    }

    log("Refreshing S3 listing for %s", tblName);
    const baseLocation = metadata.table.StorageDescriptor?.Location;
    if (!baseLocation) {
      throw new Error(`No storage location found for ${database}.${tableName}`);
    }

    const partitionKeys = (metadata.table.PartitionKeys || []).map((k) => k.Name!);
    const files = await this.listS3Files(baseLocation, partitionKeys);
    this.s3ListingCache.set(tblName, { timestamp: now, data: files });

    // Create base table for file paths
    await this.db.run(`
      CREATE OR REPLACE TABLE "${tblName}_s3_files" AS 
      SELECT path FROM (VALUES ${files.map((f) => `('${f.path}')`).join(",")}) t(path);`);

    // Create view with partition columns using appropriate extractors
    const extractors = await Promise.all(
      partitionKeys.map(async (k) => `${await this.getPartitionExtractor(k, metadata)} as ${k}`)
    );

    await this.db.run(`
      CREATE OR REPLACE TABLE "${tblName}_s3_listing" AS 
      SELECT 
        path,
        ${extractors.join(",\n        ")}
      FROM "${tblName}_s3_files";
    `);

    // Create indexes on partition columns
    for (const key of partitionKeys) {
      await this.db.run(`
        CREATE INDEX IF NOT EXISTS idx_${key} 
        ON "${tblName}_s3_listing" (${key});
      `);
    }
  }

  async getS3Locations(
    database: string,
    tableName: string,
    filter?: PartitionFilter
  ): Promise<string[]> {
    if (!this.db) await this.connect();
    if (!this.db) throw new Error("DB not connected");
    const metadata = await this.getTableMetadata(database, tableName);
    await this.ensureS3ListingTable(database, tableName, metadata);

    const key = `${database}.${tableName}`;
    let query = `SELECT DISTINCT path FROM "${key}_s3_listing"`;
    log(query);
    if (filter?.sql) {
      query += ` WHERE ${filter.sql}`;
    }

    const result = await this.db.runAndReadAll(query);
    // DuckDB returns rows as arrays, where each row is an array of values
    // First column (index 0) contains our path values
    return result.getRows().map((row) => String(row[0]));
  }

  async getFilteredS3Locations(
    database: string,
    tableName: string,
    partitionFilters: string[]
  ): Promise<string[]> {
    return this.getS3Locations(database, tableName, {
      sql: partitionFilters.join(" AND "),
    });
  }

  async createFileListVariable(
    database: string,
    tableName: string,
    filters?: string[]
  ): Promise<void> {
    if (!this.db) await this.connect();
    if (!this.db) throw new Error("DB not connected");
    const tblName = `${database}.${tableName}`;

    let query = `SELECT path FROM "${tblName}_s3_listing"`;
    if (filters && filters.length > 0) {
      query += ` WHERE ${filters.join(" AND ")}`;
    }

    // Create a list of paths as a string array
    const safeVarName = `${database}_${tableName}_files`;
    await this.db.run(`SET VARIABLE ${safeVarName} = ( SELECT list(path) FROM (${query}));`);
  }

  async convertGlueTableQuery(query: string): Promise<string> {
    if (!this.db) await this.connect();
    if (!this.db) throw new Error("DB not connected");

    if (!this.sqlTransformer) {
      this.sqlTransformer = new SqlTransformer(this.db);
    }

    return this.sqlTransformer.transformGlueTableQuery(query);
  }

  async getGlueTableViewSetupSql(
    database: string,
    tableName: string,
    query: string
  ): Promise<string[]> {
    if (!this.db) await this.connect();
    if (!this.db) throw new Error("DB not connected");
    if (!this.sqlTransformer) {
      this.sqlTransformer = new SqlTransformer(this.db);
    }

    const metadata = await this.getTableMetadata(database, tableName);
    const tblName = `${database}.${tableName}`;
    const baseLocation = metadata.table.StorageDescriptor?.Location;
    if (!baseLocation) {
      throw new Error(`No storage location found for ${tblName}`);
    }

    let partitionKeys = (metadata.table.PartitionKeys || []).map((k) => k.Name!);
    const files = await this.listS3Files(baseLocation, partitionKeys);

    // Generate all SQL statements needed
    const statements: string[] = [];

    // 1. Create base table for file paths
    statements.push(`
      CREATE OR REPLACE TABLE "${tblName}_s3_files" AS 
      SELECT path FROM (VALUES ${files.map((f) => `('${f.path}')`).join(",")}) t(path);
    `);

    // 2. Create listing table with partition columns
    const extractors = await Promise.all(
      partitionKeys.map(async (k) => `${await this.getPartitionExtractor(k, metadata)} as ${k}`)
    );

    statements.push(`
      CREATE OR REPLACE TABLE "${tblName}_s3_listing" AS 
      SELECT 
        path,
        ${extractors.join(",\n        ")}
      FROM "${tblName}_s3_files";
    `);

    // 3. Create indexes on partition columns
    for (const key of partitionKeys) {
      statements.push(`
        CREATE INDEX IF NOT EXISTS idx_${key} 
        ON "${tblName}_s3_listing" (${key});
      `);
    }

    // 4. Extract partition filters from the query and set the variable with filtered file list
    if (!this.sqlTransformer) throw new Error("SQL transformer not initialized");
    partitionKeys = (metadata.table.PartitionKeys || []).map((k) => k.Name!);
    const partitionFilters = await this.sqlTransformer.extractPartitionFilters(
      query,
      `${database}.${tableName}`,
      partitionKeys
    );

    let variableQuery = `SELECT list(path) FROM "${tblName}_s3_listing"`;
    if (partitionFilters.length > 0) {
      variableQuery += ` WHERE ${partitionFilters.join(" AND ")}`;
    }

    statements.push(`
      SET VARIABLE ${database}_${tableName}_files = (${variableQuery});
    `);

    // 5. Create views for all referenced tables
    const viewSqls = await this.sqlTransformer.getGlueTableViewSql(query);
    statements.push(...viewSqls);

    const trimmed = statements.map((stmt) => stmt.trim());
    log(trimmed);
    return trimmed;
  }
}
