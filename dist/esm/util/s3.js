import { ListObjectsV2Command } from "@aws-sdk/client-s3";
export function parseS3Path(s3Path) {
    const url = new URL(s3Path);
    return {
        bucket: url.hostname,
        prefix: url.pathname.substring(1), // remove leading '/'
    };
}
export function extractPartitionValues(path, partitionKeys) {
    const values = {};
    for (const key of partitionKeys) {
        const match = path.match(new RegExp(`${key}=([^/]+)`));
        if (match)
            values[key] = match[1];
    }
    return values;
}
export async function listS3Objects(s3cli, s3path, partitionKeys) {
    const { bucket, prefix } = parseS3Path(s3path);
    const files = [];
    // Ensure prefix ends with "/"
    const normalizedPrefix = prefix.endsWith("/") ? prefix : `${prefix}/`;
    let continuationToken;
    do {
        const command = new ListObjectsV2Command({
            Bucket: bucket,
            Prefix: normalizedPrefix,
            ContinuationToken: continuationToken,
            MaxKeys: 1000,
        });
        const response = await s3cli.send(command);
        if (response.Contents) {
            for (const object of response.Contents) {
                if (object.Key && !object.Key.includes("_$folder$")) {
                    const path = `s3://${bucket}/${object.Key}`;
                    const partitionValues = extractPartitionValues(path, partitionKeys);
                    files.push({ path, partitionValues });
                }
            }
        }
        continuationToken = response.NextContinuationToken;
    } while (continuationToken);
    return files;
}
