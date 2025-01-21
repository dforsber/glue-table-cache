import { DuckDBInstance, DuckDBConnection } from "@duckdb/node-api";
import { DuckDBResultReader } from "@duckdb/node-api/lib/DuckDBResultReader.js";
import { S3Client } from "@aws-sdk/client-s3";
import { LRUCache } from "lru-cache";
import debug from "debug";
import { S3FileInfo, CacheEntry } from "./types.js";
import { listS3Objects, mapS3PathsToInfo } from "./util/s3.js";
import { getIcebergS3FilesStmts } from "./util/iceberg.js";
import { Mutex } from "async-mutex";
import retry from "async-retry";

const log = debug("base-table-cache");
const logAws = debug("base-table-cache:aws");

type AbsLRUCache = LRUCache<string, CacheEntry<S3FileInfo[]>>;

export interface BaseTableCacheConfig {
  proxyAddress?: string;
  region?: string;
  maxEntries?: number;
  s3ListingRefresTtlhMs?: number;
  credentials?: {
    accessKeyId: string;
    secretAccessKey: string;
    sessionToken?: string;
  };
}

export abstract class BaseTableCache {
  protected db: DuckDBConnection | undefined;
  protected config: BaseTableCacheConfig;
  protected s3Client: S3Client;
  protected s3ListingCache: LRUCache<string, CacheEntry<S3FileInfo[]>>;

  constructor(config: Partial<BaseTableCacheConfig> = {}) {
    this.config = {
      region: config.region || process.env.AWS_REGION || "eu-west-1",
      maxEntries: config.maxEntries || 100,
      s3ListingRefresTtlhMs: config.s3ListingRefresTtlhMs || 3600000,
      ...config
    };
    
    const awsSdkParams = {
      region: this.config.region,
      credentials: this.config.credentials,
    };
    this.s3Client = new S3Client(awsSdkParams);

    // Initialize S3 listing cache
    this.s3ListingCache = new LRUCache({
      max: this.config.maxEntries,
      ttl: this.config.s3ListingRefresTtlhMs,
    });

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
  }

  public setCredentials(credentials: {
    accessKeyId: string;
    secretAccessKey: string;
    sessionToken?: string;
  }) {
    log("Setting credentials -- accessKeyId:", credentials.accessKeyId);
    if (credentials.secretAccessKey.length <= 0) throw new Error("No secretAccessKey");
    this.config.credentials = credentials;
  }

  protected async __connect(): Promise<DuckDBConnection> {
    if (!this.db) this.db = await (await DuckDBInstance.create(":memory:")).connect();
    if (!this.db) throw new Error("Could not create DuckDB instance");
    await this.configureConnection();
    return this.db;
  }

  protected abstract configureConnection(): Promise<void>;

  public close(): void {
    this.db?.close();
    this.db = undefined;
  }

  protected async __runAndReadAll(query: string): Promise<DuckDBResultReader> {
    if (!this.db) await this.__connect();
    if (!this.db) throw new Error("DB not connected");
    return this.db.runAndReadAll(query);
  }

  public abstract clearCache(): void;
  public abstract convertGlueTableQuery(query: string): Promise<string>;
  public abstract getGlueTableViewSetupSql(query: string): Promise<string[]>;

  protected getCacheKeyWithMutex<T>(cache: AbsLRUCache, key: string): CacheEntry<T> {
    let cached = cache.get(key);
    if (!cached) {
      cache.set(key, {
        mutex: new Mutex(),
        timestamp: Date.now(),
        data: undefined,
      });
      cached = cache.get(key);
    }
    if (!cached) throw new Error("Cache initialization failed");
    return <CacheEntry<T>>cached;
  }

  protected async __listS3IcebergFilesCached(
    s3Path: string,
    partitionKeys: string[]
  ): Promise<S3FileInfo[] | undefined> {
    const key = `${s3Path}:${partitionKeys.join(",")}`;
    const cached = this.getCacheKeyWithMutex<S3FileInfo[]>(this.s3ListingCache, key);
    if (!cached) throw new Error("Could not initialise cache entry");
    if (cached.error) delete cached.error;
    if (cached.data) {
      logAws("Using cached S3 (Iceberg) listing for %s", s3Path);
      return cached.data;
    } else {
      return cached.mutex.runExclusive(async () =>
        retry(
          async (bail, attempt) => {
            if (cached.data) return cached.data;
            if (cached.error) {
              bail(cached.error);
              return;
            }
            try {
              const sql = getIcebergS3FilesStmts(s3Path);
              logAws(sql);
              if (!this.db) await this.__connect();
              if (!this.db) throw new Error("Could not create db connection");
              const last = sql.pop();
              if (!last) throw new Error("No SQL statements generated");
              for await (const stmt of sql) {
                log(stmt);
                await this.db.runAndReadAll(stmt);
              }
              const res = (await this.db.runAndReadAll(last)).getRows();
              const listing = mapS3PathsToInfo(res.flat() as string[], partitionKeys);

              logAws("Listing Iceberg S3 files for %s", { s3Path, partitionKeys, listing });
              this.s3ListingCache.set(key, { ...cached, timestamp: Date.now(), data: listing });
              cached.error = undefined;
              return listing;
            } catch (error: any) {
              cached.error = error;
              log("__listS3IcebergFilesCached ERROR:", attempt, error);
              if (
                error?.$metadata?.httpStatusCode === 403 ||
                error?.[0]?.$metadata?.httpStatusCode === 403 ||
                error?.$metadata?.httpStatusCode === 400 ||
                error?.[0]?.$metadata?.httpStatusCode === 400 ||
                error?.message.includes("HTTP 40")
              ) {
                bail(error);
                return;
              }
              throw error;
            }
          },
          {
            retries: 3,
            minTimeout: 200,
            maxTimeout: 500,
            onRetry: (e: Error, a: number) => log("__listS3IcebergFilesCached -- retry:", a, e),
          }
        )
      );
    }
  }

  protected async __listS3FilesCached(
    s3Path: string,
    partitionKeys: string[]
  ): Promise<S3FileInfo[] | undefined> {
    const key = `${s3Path}:${partitionKeys.join(",")}`;
    const cached = this.getCacheKeyWithMutex<S3FileInfo[]>(this.s3ListingCache, key);
    if (!cached) throw new Error("Could not initialise cache entry");
    if (cached.error) delete cached.error;
    if (cached.data) {
      logAws("Using cached S3 listing for %s", s3Path);
      return cached.data;
    } else {
      return cached.mutex.runExclusive(async () =>
        retry(
          async (bail, attempt) => {
            if (cached.data) return cached.data;
            if (cached.error) {
              bail(cached.error);
              return;
            }
            try {
              logAws("Listing S3 files for %s", s3Path);
              const files: S3FileInfo[] = await listS3Objects(this.s3Client, s3Path, partitionKeys);
              this.s3ListingCache.set(key, { ...cached, timestamp: Date.now(), data: files });
              return files;
            } catch (error: any) {
              log("__listS3FilesCached ERROR:", attempt, error);
              if (
                error?.$metadata?.httpStatusCode === 403 ||
                error?.[0]?.$metadata?.httpStatusCode === 403 ||
                error?.$metadata?.httpStatusCode === 400 ||
                error?.[0]?.$metadata?.httpStatusCode === 400 ||
                error?.message.includes("HTTP 40")
              ) {
                bail(error);
                return;
              }
              throw error;
            }
          },
          {
            retries: 3,
            minTimeout: 200,
            maxTimeout: 500,
            onRetry: (e: Error, a: number) => log("__listS3FilesCached -- retry:", a, e),
          }
        )
      );
    }
  }
}
