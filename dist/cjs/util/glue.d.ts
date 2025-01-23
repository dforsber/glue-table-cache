import { GlueClient } from "@aws-sdk/client-glue";
import { CachedGlueTableMetadata, ProjectionPattern } from "../types.js";
export declare function getGlueTableMetadata(gluecli: GlueClient, DatabaseName: string, Name: string): Promise<CachedGlueTableMetadata>;
export declare function parseProjectionPatterns(parameters: Record<string, string>): {
    enabled: boolean;
    patterns: Record<string, ProjectionPattern>;
};
export declare function loadPartitionMetadata(gluecli: GlueClient, DatabaseName: string, TableName: string): Promise<{
    keys: string[];
    values: {
        values: string[];
        location: string | undefined;
    }[];
}>;
export declare function getPartitionExtractor(key: string, metadata: CachedGlueTableMetadata): Promise<string>;
export declare function convertDateFormatToRegex(format: string): string;
