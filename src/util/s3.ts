import { ListObjectsV2Command, S3Client } from "@aws-sdk/client-s3";
import { S3FileInfo } from "../types.js";

export function parseS3Path(s3Path: string): { bucket: string; prefix: string } {
  const url = new URL(s3Path);
  if (!s3Path.startsWith("s3://") && !s3Path.startsWith("S3://")) {
    throw new Error(`Not an S3 URL: ${url.protocol}`);
  }
  return {
    bucket: url.hostname,
    prefix: url.pathname.substring(1), // remove leading '/'
  };
}

export function extractPartitionValues(
  path: string,
  partitionKeys: string[]
): Record<string, string> {
  const values: Record<string, string> = {};
  for (const key of partitionKeys) {
    const match = path.match(new RegExp(`${key}=([^/]+)`));
    if (match) values[key] = match[1];
  }
  return values;
}

export function mapS3PathsToInfo(s3paths: string[], partitionKeys: string[]): S3FileInfo[] {
  return s3paths.map((path) => ({
    path,
    partitionValues: extractPartitionValues(path, partitionKeys),
  }));
}

export async function listS3Objects(
  s3cli: S3Client,
  s3path: string,
  partitionKeys: string[]
): Promise<S3FileInfo[]> {
  const { bucket: Bucket, prefix } = parseS3Path(s3path);
  const s3paths: string[] = [];

  // Ensure prefix ends with "/"
  const Prefix = prefix.endsWith("/") ? prefix : `${prefix}/`;

  let ContinuationToken: string | undefined;
  do {
    const command = new ListObjectsV2Command({ Bucket, Prefix, ContinuationToken });
    const response = await s3cli.send(command);
    s3paths.push(
      ...(response.Contents?.map((o) => `s3://${Bucket}/${o.Key}`).filter(
        (k) => !k.endsWith("_$folder$")
      ) ?? [])
    );
    ContinuationToken = response.NextContinuationToken;
  } while (ContinuationToken);

  return mapS3PathsToInfo(s3paths.filter(Boolean), partitionKeys);
}
