import { DuckDBConnection } from "@duckdb/node-api";
import { DuckDBResultReader } from "@duckdb/node-api/lib/DuckDBResultReader.js";
import { S3FileInfo, CacheEntry, AbsLRUCache } from "./types.js";
import { SqlTransformer } from "./sql-transformer.class.js";
import { S3Client } from "@aws-sdk/client-s3";
import { LRUCache } from "lru-cache";
export interface BaseTableCacheConfig {
    region?: string;
    maxEntries?: number;
    s3ListingRefresTtlhMs?: number;
    tableMetadataTtlMs?: number;
    s3ListingCache?: LRUCache<string, CacheEntry<S3FileInfo[]>>;
    proxyAddress?: string;
    sqlTransformer?: SqlTransformer;
    credentials?: {
        accessKeyId: string;
        secretAccessKey: string;
        sessionToken?: string;
    };
}
export declare abstract class BaseTableCache {
    protected db: DuckDBConnection | undefined;
    protected config: BaseTableCacheConfig;
    protected s3ListingCache: LRUCache<string, CacheEntry<S3FileInfo[]>>;
    protected s3Client?: S3Client;
    constructor(config?: Partial<BaseTableCacheConfig>);
    close(): void;
    clearCache(): void;
    setCredentials(credentials: {
        accessKeyId: string;
        secretAccessKey: string;
        sessionToken?: string;
    }): void;
    abstract convertQuery(query: string): Promise<string>;
    abstract getViewSetupSql(query: string): Promise<string[]>;
    protected __connect(): Promise<DuckDBConnection>;
    protected __runAndReadAll(query: string): Promise<DuckDBResultReader>;
    protected getCacheKeyWithMutex<T>(cache: AbsLRUCache, key: string): CacheEntry<T>;
    protected __listS3IcebergFilesCached(s3Path: string, partitionKeys: string[]): Promise<S3FileInfo[] | undefined>;
    protected __listS3FilesCached(s3Path: string, partitionKeys: string[]): Promise<S3FileInfo[] | undefined>;
}
