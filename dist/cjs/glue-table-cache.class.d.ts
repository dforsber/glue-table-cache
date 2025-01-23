import { BaseTableCache, BaseTableCacheConfig } from "./base-table-cache.abstract.js";
import { S3Client } from "@aws-sdk/client-s3";
import { type CachedGlueTableMetadata } from "./types.js";
export declare class GlueTableCache extends BaseTableCache {
    protected s3Client: S3Client;
    private tableCache;
    private glueClient;
    private sqlTransformer;
    constructor(config?: BaseTableCacheConfig);
    clearCache(): void;
    getTableMetadataCached(database: string, tableName: string): Promise<CachedGlueTableMetadata | undefined>;
    invalidateTable(database: string, tableName: string): void;
    private createGlueTableFilesVarSql;
    convertQuery(query: string): Promise<string>;
    getViewSetupSql(query: string): Promise<string[]>;
}
