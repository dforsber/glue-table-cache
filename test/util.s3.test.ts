import { extractPartitionValues, parseS3Path } from "../src/util/s3";

describe("util", () => {
  it("should parse S3 paths correctly", async () => {
    // Test basic path
    const result1 = parseS3Path("s3://bucket/prefix/path");
    expect(result1).toEqual({
      bucket: "bucket",
      prefix: "prefix/path",
    });

    // Test path with trailing slash
    const result2 = parseS3Path("s3://bucket/prefix/path/");
    expect(result2).toEqual({
      bucket: "bucket",
      prefix: "prefix/path/",
    });

    // Test path with special characters
    const result3 = parseS3Path("s3://my-bucket.123/path-with_special.chars/");
    expect(result3).toEqual({
      bucket: "my-bucket.123",
      prefix: "path-with_special.chars/",
    });
  });

  it("should extract partition values correctly", async () => {
    // Test basic partition extraction
    const values1 = extractPartitionValues("s3://bucket/path/year=2024/month=01/data.parquet", [
      "year",
      "month",
    ]);
    expect(values1).toEqual({
      year: "2024",
      month: "01",
    });

    // Test with missing partitions
    const values2 = extractPartitionValues("s3://bucket/path/year=2024/data.parquet", [
      "year",
      "month",
    ]);
    expect(values2).toEqual({
      year: "2024",
    });

    // Test with special characters in values
    const values3 = extractPartitionValues(
      "s3://bucket/path/date=2024-01-01/type=special_value.123/data.parquet",
      ["date", "type"]
    );
    expect(values3).toEqual({
      date: "2024-01-01",
      type: "special_value.123",
    });
  });
});
