import { Table } from "@aws-sdk/client-glue";
export interface S3FileInfo {
    path: string;
    partitionValues: Record<string, string>;
}
export interface CachedTableMetadata {
    timestamp: number;
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
export interface ProjectionPattern {
    type: "enum" | "integer" | "date" | "injected";
    format?: string;
    range?: [string | number, string | number];
    values?: (string | number)[];
}
export interface CacheConfig {
    region: string;
    maxEntries: number;
    forceRefreshOnError: boolean;
    glueTableMetadataTtlMs: number;
    s3ListingRefresTtlhMs: number;
    proxyAddress?: string;
}
export interface CacheEntry<T> {
    timestamp: number;
    data: T;
}
export interface TableReference {
    database: string;
    table: string;
}
