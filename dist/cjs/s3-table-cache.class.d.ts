import { BaseTableCache, BaseTableCacheConfig } from "./base-table-cache.abstract.js";
import { S3Client } from "@aws-sdk/client-s3";
import { type CachedGlueTableMetadata } from "./types.js";
export declare class S3TableCache extends BaseTableCache {
    protected s3Client: S3Client;
    private tableCache;
    private sqlTransformer;
    constructor(config?: BaseTableCacheConfig);
    clearCache(): void;
    getTableMetadataCached(database: string, tableName: string): Promise<CachedGlueTableMetadata | undefined>;
    invalidateTable(database: string, tableName: string): void;
    convertQuery(query: string): Promise<string>;
    getViewSetupSql(query: string): Promise<string[]>;
}
