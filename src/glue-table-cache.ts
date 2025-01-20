import { GlueClient } from "@aws-sdk/client-glue";
import { S3Client } from "@aws-sdk/client-s3";
import { LRUCache } from "lru-cache";
import { DuckDBInstance, DuckDBConnection } from "@duckdb/node-api";
import debug from "debug";
import { SqlTransformer } from "./sql-transformer.js";
import {
  type CachedTableMetadata,
  type CacheConfig,
  type CacheEntry,
  type S3FileInfo,
} from "./types.js";
import { DuckDBResultReader } from "@duckdb/node-api/lib/DuckDBResultReader.js";
import { listS3Objects } from "./util/s3.js";
import { getGlueTableMetadata, getPartitionExtractor } from "./util/glue.js";

export type { S3FileInfo };

const log = debug("glue-table-cache");
const logAws = debug("glue-table-cache:aws");

const defaultConfig: CacheConfig = {
  region: "eu-west-1",
  maxEntries: 100,
  forceRefreshOnError: true,
  glueTableMetadataTtlMs: 3600000, // 1 hour
  s3ListingRefresTtlhMs: 3600000, // 1 hour
  proxyAddress: undefined,
};

export class GlueTableCache {
  private tableCache: LRUCache<string, CacheEntry<CachedTableMetadata>>;
  private s3ListingCache: LRUCache<string, CacheEntry<S3FileInfo[]>>;
  private glueClient: GlueClient;
  private s3Client: S3Client;
  private db: DuckDBConnection | undefined;
  private sqlTransformer: SqlTransformer | undefined;
  private config: CacheConfig;

  constructor(config: Partial<CacheConfig> = defaultConfig) {
    log("Initializing GlueTableCache for region %s with config %O", config);
    this.config = {
      ...defaultConfig,
      ...config,
      region: config?.region || process.env.AWS_REGION || defaultConfig.region,
    };
    if (this.config.proxyAddress) {
      try {
        new URL(this.config.proxyAddress);
        if (!this.config.proxyAddress.endsWith("/")) {
          this.config.proxyAddress = this.config.proxyAddress + "/";
        }
        log("Using proxyAddress:", this.config.proxyAddress);
      } catch (err) {
        console.error(err);
        this.config.proxyAddress = undefined;
      }
    }
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
      ttl: this.config.s3ListingRefresTtlhMs,
    });
  }

  // because constructor can't be async and we don't want the clients to worry about this
  private async connect(): Promise<DuckDBConnection> {
    if (!this.db) this.db = await (await DuckDBInstance.create(":memory:")).connect();
    if (!this.sqlTransformer) this.sqlTransformer = new SqlTransformer(this.db);
    return this.db;
  }

  public clearCache(): void {
    this.tableCache.clear();
    this.s3ListingCache.clear();
  }

  public close(): void {
    this.db?.close();
    this.db = undefined;
    this.sqlTransformer = undefined;
  }

  public async getTableMetadataCached(
    database: string,
    tableName: string
  ): Promise<CachedTableMetadata> {
    const key = `${database}_${tableName}`;
    log("Getting table metadata for %s", key);
    const cached = this.tableCache.get(key);

    if (!cached) {
      log("Cache miss for %s, refreshing...", key);
      try {
        logAws("Fetching table metadata from AWS for %s.%s", database, tableName);
        const metadata = await getGlueTableMetadata(this.glueClient, database, tableName);
        const now = Date.now();
        const entry: CacheEntry<CachedTableMetadata> = {
          timestamp: now,
          data: metadata,
        };
        this.tableCache.set(key, entry);
        return metadata;
      } catch (error) {
        if (this.config.forceRefreshOnError) this.tableCache.delete(`${database}_${tableName}`);
        throw error;
      }
    }
    log("Cache hit for %s", key);
    return cached.data;
  }

  public invalidateTable(database: string, tableName: string): void {
    const key = `${database}_${tableName}`;
    this.tableCache.delete(key);
    // Also invalidate any S3 listings for this table
    for (const cacheKey of this.s3ListingCache.keys()) {
      if (cacheKey.includes(key)) {
        this.s3ListingCache.delete(cacheKey);
      }
    }
  }

  public async createGlueTableFilesVarSql(
    database: string,
    tableName: string,
    filters?: string[]
  ): Promise<string> {
    if (!this.db) await this.connect();
    if (!this.db) throw new Error("DB not connected");
    if (!this.sqlTransformer) this.sqlTransformer = new SqlTransformer(this.db);
    if (!this.sqlTransformer) throw new Error("SQL transformer not initialized");
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

  public async convertGlueTableQuery(query: string): Promise<string> {
    if (!this.db) await this.connect();
    if (!this.db) throw new Error("DB not connected");
    if (!this.sqlTransformer) this.sqlTransformer = new SqlTransformer(this.db);
    if (!this.sqlTransformer) throw new Error("SQL transformer not initialized");

    const setupSql = await this.getGlueTableViewSetupSql(query);
    const transformedQuery = await this.sqlTransformer.transformGlueTableQuery(query);
    return setupSql.join("") + transformedQuery;
  }

  public async getGlueTableViewSetupSql(query: string): Promise<string[]> {
    if (!this.db) await this.connect();
    if (!this.db) throw new Error("DB not connected");
    if (!this.sqlTransformer) this.sqlTransformer = new SqlTransformer(this.db);
    if (!this.sqlTransformer) throw new Error("SQL transformer not initialized");

    // Generate all SQL statements needed
    const statements: string[] = [];

    // Get Glue Table references from the query
    const tableRefs = await this.sqlTransformer.getQueryGlueTableRefs(query);
    log("Found Glue Table references: %O", tableRefs);
    await Promise.all(
      tableRefs.map(async ({ database, table }) => {
        log("Found Glue Table reference: %s", { database, table });

        const metadata = await this.getTableMetadataCached(database, table);
        const tblName = `${database}_${table}`;
        const baseLocation = metadata.table.StorageDescriptor?.Location;
        if (!baseLocation) {
          throw new Error(`No storage location found for ${tblName}`);
        }

        let partitionKeys = (metadata.table.PartitionKeys || []).map((k) => k.Name!);
        const files = await this.__listS3FilesCached(baseLocation, partitionKeys);

        // 1. Create base table for file paths
        statements.push(
          `CREATE OR REPLACE TABLE "${tblName}_s3_files" AS ` +
            `SELECT path FROM (VALUES ${files.length ? files.map((f) => `('${f.path}')`).join(",") : "( '' )"}) t(path);`
        );

        // 2. Create listing table with partition columns
        const extractors = await Promise.all(
          partitionKeys.map(async (k) => `${await getPartitionExtractor(k, metadata)} as ${k}`)
        );

        statements.push(
          `CREATE OR REPLACE TABLE "${tblName}_s3_listing" AS ` +
            `SELECT path, ${extractors.join(", ")} FROM "${tblName}_s3_files";`
        );

        // 3. Create indexes on partition columns
        for (const key of partitionKeys) {
          statements.push(
            `CREATE INDEX IF NOT EXISTS idx_${key} ON "${tblName}_s3_listing" (${key});`
          );
        }

        // 4. Extract partition filters from the query and set the variable with filtered file list
        if (!this.sqlTransformer) throw new Error("SQL transformer not initialized"); // make TS happy
        partitionKeys = (metadata.table.PartitionKeys || []).map((k) => k.Name!);
        const partitionFilters = await this.sqlTransformer.extractPartitionFilters(
          query,
          partitionKeys
        );

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
        if (glueTableViewSql) statements.push(glueTableViewSql);

        const viewSqls = await this.sqlTransformer.getGlueTableViewSql(query, files.length);
        statements.push(...viewSqls);
      })
    );

    const trimmed = statements.map((stmt) => stmt.trim());
    log(trimmed);
    return trimmed;
  }

  private async __listS3FilesCached(
    s3Path: string,
    partitionKeys: string[]
  ): Promise<S3FileInfo[]> {
    const cacheKey = `${s3Path}:${partitionKeys.join(",")}`;
    const cached = this.s3ListingCache.get(cacheKey);

    if (cached) {
      logAws("Using cached S3 listing for %s", s3Path);
      return cached.data;
    }

    logAws("Listing S3 files for %s", s3Path);
    const files: S3FileInfo[] = await listS3Objects(this.s3Client, s3Path, partitionKeys);

    // Cache the results
    const now = Date.now();
    this.s3ListingCache.set(cacheKey, { timestamp: now, data: files });

    return files;
  }

  // tests use this
  private async __runAndReadAll(query: string): Promise<DuckDBResultReader> {
    if (!this.db) await this.connect();
    if (!this.db) throw new Error("DB not connected");
    return this.db.runAndReadAll(query);
  }
}
