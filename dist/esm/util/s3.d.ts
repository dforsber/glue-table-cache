import { S3Client } from "@aws-sdk/client-s3";
import { S3FileInfo } from "../types.js";
export declare function parseS3Path(s3Path: string): {
    bucket: string;
    prefix: string;
};
export declare function extractPartitionValues(path: string, partitionKeys: string[]): Record<string, string>;
export declare function listS3Objects(s3cli: S3Client, s3path: string, partitionKeys: string[]): Promise<S3FileInfo[]>;
