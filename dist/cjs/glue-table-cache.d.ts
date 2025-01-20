import { type CachedTableMetadata, type CacheConfig, type S3FileInfo } from "./types.js";
export type { S3FileInfo };
export declare class GlueTableCache {
    private tableCache;
    private s3ListingCache;
    private glueClient;
    private s3Client;
    private db;
    private sqlTransformer;
    private config;
    constructor(config?: Partial<CacheConfig>);
    private connect;
    clearCache(): void;
    close(): void;
    private runAndReadAll;
    getTableMetadataCached(database: string, tableName: string): Promise<CachedTableMetadata>;
    invalidateTable(database: string, tableName: string): void;
    private listS3FilesCached;
    createGlueTableFilesVarSql(database: string, tableName: string, filters?: string[]): Promise<string>;
    convertGlueTableQuery(query: string): Promise<string>;
    private getGlueTableViewSetupSql;
}
