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
    setCredentials(credentials: {
        accessKeyId: string;
        secretAccessKey: string;
        sessionToken?: string;
    }): void;
    private __connect;
    clearCache(): void;
    close(): void;
    private getCacheKeyWithMutex;
    getTableMetadataCached(database: string, tableName: string): Promise<CachedTableMetadata | undefined>;
    invalidateTable(database: string, tableName: string): void;
    createGlueTableFilesVarSql(database: string, tableName: string, filters?: string[]): Promise<string>;
    convertGlueTableQuery(query: string): Promise<string>;
    getGlueTableViewSetupSql(query: string): Promise<string[]>;
    private __listS3IcebergFilesCached;
    private __listS3FilesCached;
    private __runAndReadAll;
}
