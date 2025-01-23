import { Table } from "@aws-sdk/client-glue";
import { Mutex } from "async-mutex";
import { LRUCache } from "lru-cache";
export type AbsLRUCache = LRUCache<string, CacheEntry<CachedGlueTableMetadata>> | LRUCache<string, CacheEntry<CachedS3TableMetadata>> | LRUCache<string, CacheEntry<S3FileInfo[]>>;
export declare enum ETableType {
    HIVE = "HIVE",
    ICEBERG = "ICEBERG",
    HUDI = "HUDI",
    DELTA = "DELTA",
    GLUE_PROJECTED = "GLUE_PROJECTED",
    UNPARTITIONED = "UNPARTITIONED",
    S3_TABLE = "S3_TABLE"
}
export interface S3FileInfo {
    path: string;
    partitionValues: Record<string, string>;
}
export interface CachedGlueTableMetadata {
    timestamp: number;
    tableType: ETableType;
    table: Table;
    partitionMetadata?: {
        keys: string[];
        values: Array<{
            values?: string[];
            location?: string;
        }>;
    };
    projectionPatterns?: {
        enabled: boolean;
        patterns: Record<string, ProjectionPattern>;
    };
}
export interface CachedS3TableMetadata {
    timestamp: number;
    tableType: ETableType;
}
export interface ProjectionPattern {
    type: "enum" | "integer" | "date" | "injected";
    format?: string;
    range?: [string | number, string | number];
    values?: (string | number)[];
}
export interface CacheConfig {
    region: string;
    maxEntries: number;
    glueTableMetadataTtlMs: number;
    s3ListingRefresTtlhMs: number;
    credentials?: {
        accessKeyId: string;
        secretAccessKey: string;
        sessionToken?: string;
    };
    proxyAddress?: string;
}
export interface CacheEntry<T> {
    mutex: Mutex;
    error?: any;
    timestamp: number;
    data: T | undefined;
}
export interface TableReference {
    database: string;
    table: string;
}
