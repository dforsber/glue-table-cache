import { type CachedTableMetadata, type CacheConfig, type S3FileInfo } from "./types.js";
export type { S3FileInfo };
interface PartitionFilter {
    sql: string;
    parameters?: unknown[];
}
export declare class GlueTableCache {
    private tableCache;
    private s3ListingCache;
    private glueClient;
    private s3Client;
    private db;
    private sqlTransformer;
    private config;
    constructor(config?: Partial<CacheConfig>);
    private runAndReadAll;
    private connect;
    close(): void;
    getTableMetadata(database: string, tableName: string): Promise<CachedTableMetadata>;
    private refreshTableMetadata;
    private parseProjectionValue;
    private parseProjectionPatterns;
    private loadPartitionMetadata;
    clearCache(): void;
    invalidateTable(database: string, tableName: string): void;
    private parseS3Path;
    private extractPartitionValues;
    private listS3Files;
    private getPartitionExtractor;
    private convertDateFormatToRegex;
    private ensureS3ListingTable;
    getS3Locations(database: string, tableName: string, filter?: PartitionFilter): Promise<string[]>;
    getFilteredS3Locations(database: string, tableName: string, partitionFilters: string[]): Promise<string[]>;
    createGlueTableFilesVarSql(database: string, tableName: string, filters?: string[]): Promise<string>;
    convertGlueTableQuery(query: string): Promise<string>;
    private getGlueTableViewSetupSql;
}
