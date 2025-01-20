import { extractPartitionValues, parseS3Path, listS3Objects } from "../src/util/s3";
import { S3Client, ListObjectsV2Command } from "@aws-sdk/client-s3";
import { mockClient } from "aws-sdk-client-mock";

describe("S3 Utilities", () => {
  describe("parseS3Path", () => {
    it("should parse basic S3 paths", () => {
      const result = parseS3Path("s3://bucket/prefix/path");
      expect(result).toEqual({
        bucket: "bucket",
        prefix: "prefix/path",
      });
    });

    it("should handle paths with trailing slashes", () => {
      const result = parseS3Path("s3://bucket/prefix/path/");
      expect(result).toEqual({
        bucket: "bucket",
        prefix: "prefix/path/",
      });
    });

    it("should handle buckets with special characters", () => {
      const result = parseS3Path("s3://my-bucket.123/path-with_special.chars/");
      expect(result).toEqual({
        bucket: "my-bucket.123",
        prefix: "path-with_special.chars/",
      });
    });

    it("should handle empty prefix", () => {
      const result = parseS3Path("s3://bucket/");
      expect(result).toEqual({
        bucket: "bucket",
        prefix: "",
      });
    });

    it("should throw error for invalid S3 URI", () => {
      expect(() => parseS3Path("invalid://bucket/path")).toThrow();
      expect(() => parseS3Path("s3:/bucket/path")).toThrow();
    });
  });

  describe("extractPartitionValues", () => {
    it("should extract basic partition values", () => {
      const values = extractPartitionValues("s3://bucket/path/year=2024/month=01/data.parquet", [
        "year",
        "month",
      ]);
      expect(values).toEqual({
        year: "2024",
        month: "01",
      });
    });

    it("should handle missing partitions", () => {
      const values = extractPartitionValues("s3://bucket/path/year=2024/data.parquet", [
        "year",
        "month",
      ]);
      expect(values).toEqual({
        year: "2024",
      });
    });

    it("should handle special characters in partition values", () => {
      const values = extractPartitionValues(
        "s3://bucket/path/date=2024-01-01/type=special_value.123/data.parquet",
        ["date", "type"]
      );
      expect(values).toEqual({
        date: "2024-01-01",
        type: "special_value.123",
      });
    });

    it("should handle empty partition keys array", () => {
      const values = extractPartitionValues("s3://bucket/path/year=2024/month=01/data.parquet", []);
      expect(values).toEqual({});
    });

    it("should handle paths without partitions", () => {
      const values = extractPartitionValues("s3://bucket/path/data.parquet", ["year", "month"]);
      expect(values).toEqual({});
    });
  });

  describe("listS3Objects", () => {
    const s3Mock = mockClient(S3Client);

    beforeEach(() => {
      s3Mock.reset();
    });

    it("should list objects with pagination", async () => {
      s3Mock
        .on(ListObjectsV2Command, {
          Bucket: "test-bucket",
          Prefix: "test-prefix/",
        })
        .resolvesOnce({
          Contents: [
            { Key: "test-prefix/year=2024/file1.parquet" },
            { Key: "test-prefix/year=2024/file2.parquet" },
          ],
          NextContinuationToken: "token1",
        })
        .resolvesOnce({
          Contents: [{ Key: "test-prefix/year=2024/file3.parquet" }],
        });

      const result = await listS3Objects(
        s3Mock as unknown as S3Client,
        "s3://test-bucket/test-prefix",
        ["year"]
      );

      expect(result).toHaveLength(3);
      expect(result[0].path).toBe("s3://test-bucket/test-prefix/year=2024/file1.parquet");
      expect(result[0].partitionValues).toEqual({ year: "2024" });
    });

    it("should handle empty response", async () => {
      s3Mock.on(ListObjectsV2Command).resolves({
        Contents: [],
      });

      const result = await listS3Objects(
        s3Mock as unknown as S3Client,
        "s3://test-bucket/test-prefix",
        ["year"]
      );

      expect(result).toHaveLength(0);
    });

    it("should filter out folder markers", async () => {
      s3Mock.on(ListObjectsV2Command).resolves({
        Contents: [
          { Key: "test-prefix/year=2024/file1.parquet" },
          { Key: "test-prefix/year=2024/_$folder$" },
        ],
      });

      const result = await listS3Objects(
        s3Mock as unknown as S3Client,
        "s3://test-bucket/test-prefix",
        ["year"]
      );

      expect(result).toHaveLength(1);
      expect(result[0].path).toBe("s3://test-bucket/test-prefix/year=2024/file1.parquet");
    });
  });
});
