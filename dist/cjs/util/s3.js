"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.parseS3Path = parseS3Path;
exports.extractPartitionValues = extractPartitionValues;
exports.mapS3PathsToInfo = mapS3PathsToInfo;
exports.listS3Objects = listS3Objects;
const client_s3_1 = require("@aws-sdk/client-s3");
function parseS3Path(s3Path) {
    const url = new URL(s3Path);
    if (!s3Path.startsWith("s3://") && !s3Path.startsWith("S3://")) {
        throw new Error(`Not an S3 URL: ${url.protocol}`);
    }
    return {
        bucket: url.hostname,
        prefix: url.pathname.substring(1), // remove leading '/'
    };
}
function extractPartitionValues(path, partitionKeys) {
    const values = {};
    for (const key of partitionKeys) {
        const match = path.match(new RegExp(`${key}=([^/]+)`));
        if (match)
            values[key] = match[1];
    }
    return values;
}
function mapS3PathsToInfo(s3paths, partitionKeys) {
    return s3paths.map((path) => ({
        path,
        partitionValues: extractPartitionValues(path, partitionKeys),
    }));
}
async function listS3Objects(s3cli, s3path, partitionKeys) {
    const { bucket: Bucket, prefix } = parseS3Path(s3path);
    const s3paths = [];
    // Ensure prefix ends with "/"
    const Prefix = prefix.endsWith("/") ? prefix : `${prefix}/`;
    let ContinuationToken;
    do {
        const command = new client_s3_1.ListObjectsV2Command({ Bucket, Prefix, ContinuationToken });
        const response = await s3cli.send(command);
        s3paths.push(...(response.Contents?.map((o) => `s3://${Bucket}/${o.Key}`).filter((k) => !k.endsWith("_$folder$")) ?? []));
        ContinuationToken = response.NextContinuationToken;
    } while (ContinuationToken);
    return mapS3PathsToInfo(s3paths.filter(Boolean), partitionKeys);
}
