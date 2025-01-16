/* eslint-disable jest/no-disabled-tests */
/* eslint-disable @typescript-eslint/no-explicit-any */
import { GetPartitionsCommand, GetTableCommand, GlueClient } from "@aws-sdk/client-glue";
import { GlueTableCache } from "../src/glue-table-cache";
import { mockClient } from "aws-sdk-client-mock";

const glueMock = mockClient(GlueClient);

describe("GlueTableCache", () => {
  let cache: GlueTableCache;

  beforeEach(() => {
    glueMock.reset();
    cache = new GlueTableCache("eu-west-1");
  });

  afterEach(async () => {
    await cache.close();
  });

  it("should fetch and cache standard partitioned table", async () => {
    // Mock Glue response for standard partitioned table
    glueMock.on(GetTableCommand).resolves({
      Table: {
        Name: "test_table",
        DatabaseName: "test_db",
        PartitionKeys: [
          { Name: "year", Type: "string" },
          { Name: "month", Type: "string" },
        ],
        Parameters: {},
      },
    });

    glueMock.on(GetPartitionsCommand).resolves({
      Partitions: [
        {
          Values: ["2024", "01"],
          StorageDescriptor: {
            Location: "s3://bucket/path/year=2024/month=01",
          },
        },
        {
          Values: ["2024", "02"],
          StorageDescriptor: {
            Location: "s3://bucket/path/year=2024/month=02",
          },
        },
      ],
    });

    const cache = new GlueTableCache("eu-west-1", {
      ttlMs: 3600000,
      maxEntries: 10,
      forceRefreshOnError: true,
      s3ListingRefreshMs: 60000, // Add this line
    });

    // First call should hit AWS
    const metadata1 = await cache.getTableMetadata("test_db", "test_table");
    expect(metadata1.table.Name).toBe("test_table");
    expect(metadata1.partitionMetadata?.values.length).toBe(2);
    expect(glueMock.calls().length).toBe(2); // GetTable + GetPartitions

    // Second call should use cache
    const metadata2 = await cache.getTableMetadata("test_db", "test_table");
    expect(metadata2.table.Name).toBe("test_table");
    expect(glueMock.calls().length).toBe(2); // No additional AWS calls
  });

  it("should handle partition projection tables", async () => {
    // Mock Glue response for projection-enabled table
    glueMock.on(GetTableCommand).resolves({
      Table: {
        Name: "test_projection",
        DatabaseName: "test_db",
        PartitionKeys: [{ Name: "dt", Type: "string" }],
        Parameters: {
          "projection.enabled": "true",
          "projection.dt.type": "date",
          "projection.dt.format": "yyyy-MM-dd",
          "projection.dt.range": '["2024-01-01","2024-12-31"]',
          "storage.location.template": "s3://bucket/path/${dt}",
        },
      },
    });

    const cache = new GlueTableCache("eu-west-1", {
      ttlMs: 3600000,
      maxEntries: 100,
      forceRefreshOnError: true,
      s3ListingRefreshMs: 60000,
    });
    const metadata = await cache.getTableMetadata("test_db", "test_projection");

    expect(metadata.projectionPatterns?.enabled).toBe(true);
    expect(metadata.projectionPatterns?.patterns.dt).toBeDefined();
    expect(metadata.projectionPatterns?.patterns.dt.type).toBe("date");
    expect(glueMock.calls().length).toBe(1); // Only GetTable, no GetPartitions
  });

  it("should handle cache clearing", async () => {
    glueMock.on(GetTableCommand).resolves({
      Table: {
        Name: "test_table",
        DatabaseName: "test_db",
        Parameters: {},
      },
    });

    const cache = new GlueTableCache("eu-west-1", {
      ttlMs: 3600000,
      maxEntries: 100,
      forceRefreshOnError: true,
      s3ListingRefreshMs: 60000,
    });
    await cache.getTableMetadata("test_db", "test_table");
    expect(glueMock.calls().length).toBe(1);

    cache.clearCache();
    await cache.getTableMetadata("test_db", "test_table");
    expect(glueMock.calls().length).toBe(2);
  });

  it("should handle cache invalidation", async () => {
    glueMock.on(GetTableCommand).resolves({
      Table: {
        Name: "test_table",
        DatabaseName: "test_db",
        Parameters: {},
      },
    });

    const cache = new GlueTableCache("eu-west-1", {
      ttlMs: 100, // Short TTL for testing
      maxEntries: 10,
      forceRefreshOnError: true,
      s3ListingRefreshMs: 60000, // Add this line
    });

    // First call
    await cache.getTableMetadata("test_db", "test_table");
    expect(glueMock.calls().length).toBe(1);

    // Wait for TTL to expire
    await new Promise((resolve) => setTimeout(resolve, 150));

    // Second call should refresh
    await cache.getTableMetadata("test_db", "test_table");
    expect(glueMock.calls().length).toBe(2);
  });
});

describe("Complete View Setup", () => {
  it("should generate complete view setup SQL", async () => {
    // Mock Glue response
    glueMock.on(GetTableCommand).resolves({
      Table: {
        Name: "mytable",
        DatabaseName: "mydb",
        StorageDescriptor: {
          Location: "s3://test-bucket/mydb/mytable/",
          Columns: [],
        },
        PartitionKeys: [
          { Name: "year", Type: "string" },
          { Name: "month", Type: "string" },
        ],
        Parameters: {},
        TableType: "EXTERNAL_TABLE",
      },
    });

    glueMock.on(GetPartitionsCommand).resolves({
      Partitions: [
        {
          Values: ["2024", "01"],
          StorageDescriptor: {
            Location: "s3://test-bucket/mydb/mytable/year=2024/month=01",
          },
        },
      ],
    });

    const cache = new GlueTableCache("eu-west-1");

    // Mock S3 listing to return some test files
    const s3Mock = jest.spyOn(cache as any, "listS3Files");
    s3Mock.mockResolvedValue([
      {
        path: "s3://test-bucket/mydb/mytable/year=2024/month=01/data.parquet",
        partitionValues: { year: "2024", month: "01" },
      },
    ]);

    const statements = await cache.getGlueTableViewSetupSql(
      "mydb",
      "mytable",
      "SELECT * FROM glue.mydb.mytable"
    );

    expect(statements).toHaveLength(6); // Base table, listing table, index, variable, view
    expect(statements[0]).toContain('CREATE OR REPLACE TABLE "mydb.mytable_s3_files"');
    expect(statements[1]).toContain('CREATE OR REPLACE TABLE "mydb.mytable_s3_listing"');
    expect(statements[2]).toContain("CREATE INDEX");
    expect(statements[4]).toContain("SET VARIABLE mydb_mytable_files");
    expect(statements[5]).toContain("CREATE OR REPLACE VIEW mydb_mytable_gview");
  });
});

describe("GlueTableCache Partition Extraction", () => {
  let cache: GlueTableCache;

  beforeEach(async () => {
    glueMock.reset();
    cache = new GlueTableCache("eu-west-1");
  });

  afterEach(async () => {
    await cache.close();
  });

  it("should handle Hive-style partitioning", async () => {
    // Mock Glue response for Hive partitioned table
    glueMock.on(GetTableCommand).resolves({
      Table: {
        Name: "hive_table",
        DatabaseName: "test_db",
        PartitionKeys: [
          { Name: "year", Type: "string" },
          { Name: "month", Type: "string" },
          { Name: "day", Type: "string" },
        ],
        StorageDescriptor: {
          Location: "s3://test-bucket/hive-data/",
        },
        Parameters: {},
      },
    });

    // Add mock for GetPartitionsCommand
    glueMock.on(GetPartitionsCommand).resolves({
      Partitions: [
        {
          Values: ["2024", "01", "15"],
          StorageDescriptor: {
            Location: "s3://test-bucket/hive-data/year=2024/month=01/day=15",
          },
        },
        {
          Values: ["2024", "02", "01"],
          StorageDescriptor: {
            Location: "s3://test-bucket/hive-data/year=2024/month=02/day=01",
          },
        },
      ],
    });

    await cache.getTableMetadata("test_db", "hive_table");

    // Test file paths
    const testFiles = [
      "s3://test-bucket/hive-data/year=2024/month=01/day=15/data.parquet",
      "s3://test-bucket/hive-data/year=2024/month=02/day=01/data.parquet",
    ];

    // Mock S3 listing
    const s3Mock = jest.spyOn(cache as any, "listS3Files");
    s3Mock.mockResolvedValue(
      testFiles.map((path) => ({
        path,
        partitionValues: {},
      }))
    );

    await cache.getFilteredS3Locations("test_db", "hive_table", ["year = '2024'", "month = '01'"]);

    // Verify the SQL view was created correctly
    const result = await (cache as any)?.runAndReadAll(`
      SELECT year, month, day 
      FROM "test_db.hive_table_s3_listing" 
      WHERE path = '${testFiles[0]}'
    `);

    const row = result.getRows()[0];
    expect(row).toEqual(["2024", "01", "15"]);
  });

  it("should handle date projection patterns", async () => {
    glueMock.on(GetTableCommand).resolves({
      Table: {
        Name: "date_projection",
        DatabaseName: "test_db",
        PartitionKeys: [{ Name: "dt", Type: "string" }],
        StorageDescriptor: {
          Location: "s3://test-bucket/date-data/",
        },
        Parameters: {
          "projection.enabled": "true",
          "projection.dt.type": "date",
          "projection.dt.format": "yyyy/MM/dd",
          "projection.dt.range": '["2024-01-01","2024-12-31"]',
        },
      },
    });

    await cache.getTableMetadata("test_db", "date_projection");

    const testFiles = [
      "s3://test-bucket/date-data/2024/01/15/data.parquet",
      "s3://test-bucket/date-data/2024/02/01/data.parquet",
    ];

    const s3Mock = jest.spyOn(cache as any, "listS3Files");
    s3Mock.mockResolvedValue(
      testFiles.map((path) => ({
        path,
        partitionValues: {},
      }))
    );

    await cache.getFilteredS3Locations("test_db", "date_projection", []);

    const result = await (cache as any)?.runAndReadAll(`
      SELECT dt 
      FROM "test_db.date_projection_s3_listing" 
      WHERE path = '${testFiles[0]}'
    `);

    const row = result.getRows()[0];
    expect(row[0]).toBe("2024/01/15");
  });

  it("should handle integer projection patterns", async () => {
    glueMock.on(GetTableCommand).resolves({
      Table: {
        Name: "integer_projection",
        DatabaseName: "test_db",
        PartitionKeys: [{ Name: "year", Type: "integer" }],
        StorageDescriptor: {
          Location: "s3://test-bucket/int-data/",
        },
        Parameters: {
          "projection.enabled": "true",
          "projection.year.type": "integer",
          "projection.year.range": "[2020,2024]",
        },
      },
    });

    await cache.getTableMetadata("test_db", "integer_projection");

    const testFiles = [
      "s3://test-bucket/int-data/2024/data.parquet",
      "s3://test-bucket/int-data/2023/data.parquet",
    ];

    const s3Mock = jest.spyOn(cache as any, "listS3Files");
    s3Mock.mockResolvedValue(
      testFiles.map((path) => ({
        path,
        partitionValues: {},
      }))
    );

    await cache.getFilteredS3Locations("test_db", "integer_projection", ["year = 2024"]);

    const result = await (cache as any)?.runAndReadAll(`
      SELECT year 
      FROM "test_db.integer_projection_s3_listing" 
      WHERE path = '${testFiles[0]}'
    `);

    const row = result.getRows()[0];
    expect(row[0]).toBe(2024);
  });

  it("should handle enum projection patterns", async () => {
    glueMock.on(GetTableCommand).resolves({
      Table: {
        Name: "enum_projection",
        DatabaseName: "test_db",
        PartitionKeys: [{ Name: "region", Type: "string" }],
        StorageDescriptor: {
          Location: "s3://test-bucket/enum-data/",
        },
        Parameters: {
          "projection.enabled": "true",
          "projection.region.type": "enum",
          "projection.region.values": '["us-east-1","eu-west-1"]',
        },
      },
    });

    await cache.getTableMetadata("test_db", "enum_projection");

    const testFiles = [
      "s3://test-bucket/enum-data/us-east-1/data.parquet",
      "s3://test-bucket/enum-data/eu-west-1/data.parquet",
    ];

    const s3Mock = jest.spyOn(cache as any, "listS3Files");
    s3Mock.mockResolvedValue(
      testFiles.map((path) => ({
        path,
        partitionValues: {},
      }))
    );

    await cache.getFilteredS3Locations("test_db", "enum_projection", ["region = 'us-east-1'"]);

    const result = await (cache as any)?.runAndReadAll(`
      SELECT region 
      FROM "test_db.enum_projection_s3_listing" 
      WHERE path = '${testFiles[0]}'
    `);

    const row = result.getRows()[0];
    expect(row[0]).toBe("us-east-1");
  });

  it("should convert Glue table references to parquet_scan", async () => {
    glueMock.on(GetTableCommand).resolves({
      Table: {
        Name: "test_table",
        DatabaseName: "test_db",
        StorageDescriptor: {
          Location: "s3://test-bucket/data/",
        },
      },
    });

    await cache.getTableMetadata("test_db", "test_table");

    const testFiles = [
      "s3://test-bucket/data/file1.parquet",
      "s3://test-bucket/data/file2.parquet",
    ];

    const s3Mock = jest.spyOn(cache as any, "listS3Files");
    s3Mock.mockResolvedValue(
      testFiles.map((path) => ({
        path,
        partitionValues: {},
      }))
    );

    const inputQuery = `
      SELECT col1, col2 
      FROM glue.test_db.test_table 
      WHERE col1 > 0
    `;

    const convertedQuery = await cache.convertGlueTableQuery(inputQuery);
    expect(convertedQuery).toContain("parquet_scan(getvariable('test_db_test_table_files'))");
    expect(convertedQuery).not.toContain("glue.test_db.test_table");
  });

  it("should handle missing storage location", async () => {
    glueMock.on(GetTableCommand).resolves({
      Table: {
        Name: "no_location",
        DatabaseName: "test_db",
      },
    });

    await expect(async () => {
      await cache.getFilteredS3Locations("test_db", "no_location", []);
    }).rejects.toThrow("No storage location found for test_db.no_location");
  });

  it("should handle error cases", async () => {
    glueMock.on(GetTableCommand).rejects(new Error("AWS Error"));

    const cache = new GlueTableCache("eu-west-1", {
      forceRefreshOnError: true,
    });

    await expect(cache.getTableMetadata("test_db", "error_table")).rejects.toThrow("AWS Error");
    expect(glueMock.calls().length).toBe(1);
  });

  it("should parse S3 paths correctly", async () => {
    const cache = new GlueTableCache("eu-west-1", {
      ttlMs: 3600000,
      maxEntries: 100,
      forceRefreshOnError: true,
      s3ListingRefreshMs: 60000,
    });
    const result = (cache as any).parseS3Path("s3://bucket/prefix/path");

    expect(result).toEqual({
      bucket: "bucket",
      prefix: "prefix/path",
    });
  });

  it("should handle file list variable creation", async () => {
    glueMock.on(GetTableCommand).resolves({
      Table: {
        Name: "test_table",
        DatabaseName: "test_db",
        StorageDescriptor: {
          Location: "s3://test-bucket/data/",
        },
      },
    });

    const cache = new GlueTableCache("eu-west-1", {
      ttlMs: 3600000,
      maxEntries: 100,
      forceRefreshOnError: true,
      s3ListingRefreshMs: 60000,
    });

    const s3Mock = jest.spyOn(cache as any, "listS3Files");
    s3Mock.mockResolvedValue([
      { path: "s3://test-bucket/data/file1.parquet" },
      { path: "s3://test-bucket/data/file2.parquet" },
    ]);

    // First get the table metadata and ensure S3 listing exists
    const metadata = await cache.getTableMetadata("test_db", "test_table");
    await (cache as any).ensureS3ListingTable("test_db", "test_table", metadata);

    await cache.createFileListVariable("test_db", "test_table");
    const result = await (cache as any)?.runAndReadAll(
      "SELECT getvariable('test_db_test_table_files')"
    );
    const value = result.getRows()[0][0];
    if (value === null) {
      throw new Error("Expected file list variable to be non-null");
    }
    expect((value as any)?.items).toContain("s3://test-bucket/data/file1.parquet");
  });

  it("should handle S3 listing cache", async () => {
    glueMock.on(GetTableCommand).resolves({
      Table: {
        Name: "test_table",
        DatabaseName: "test_db",
        StorageDescriptor: {
          Location: "s3://test-bucket/data/",
        },
      },
    });

    const cache = new GlueTableCache("eu-west-1", {
      ttlMs: 3600000,
      maxEntries: 100,
      forceRefreshOnError: true,
      s3ListingRefreshMs: 100,
    });

    const s3Mock = jest.spyOn(cache as any, "listS3Files");
    s3Mock.mockResolvedValue([{ path: "s3://test-bucket/data/file1.parquet" }]);

    // First call should hit S3
    await cache.getFilteredS3Locations("test_db", "test_table", []);
    expect(s3Mock).toHaveBeenCalledTimes(1);

    // Second immediate call should use cache
    await cache.getFilteredS3Locations("test_db", "test_table", []);
    expect(s3Mock).toHaveBeenCalledTimes(1);

    // Wait for cache to expire
    await new Promise((resolve) => setTimeout(resolve, 150));

    // Call after expiry should hit S3 again
    await cache.getFilteredS3Locations("test_db", "test_table", []);
    expect(s3Mock).toHaveBeenCalledTimes(2);
  });

  it.skip("should handle injected projection from query filters", async () => {
    glueMock.on(GetTableCommand).resolves({
      Table: {
        Name: "injected_projection",
        DatabaseName: "test_db",
        PartitionKeys: [{ Name: "device_id", Type: "string" }],
        StorageDescriptor: {
          Location: "s3://test-bucket/device-data/",
        },
        Parameters: {
          "projection.enabled": "true",
          "projection.device_id.type": "injected",
          "storage.location.template": "s3://test-bucket/device-data/${device_id}",
        },
      },
    });

    const deviceIds = [
      "4a770164-0392-4a41-8565-40ed8cec737e",
      "f71d12cf-f01f-4877-875d-128c23cbde17",
    ];

    const testFiles = deviceIds.map((id) => `s3://test-bucket/device-data/${id}/data.parquet`);

    const s3Mock = jest.spyOn(cache as any, "listS3Files");
    s3Mock.mockResolvedValue(
      testFiles.map((path) => ({
        path,
        partitionValues: {},
      }))
    );

    await cache.getFilteredS3Locations("test_db", "injected_projection", [
      `device_id IN ('${deviceIds.join("','")}')`,
    ]);

    const result = await (cache as any)?.runAndReadAll(`
      SELECT device_id 
      FROM "test_db.injected_projection_s3_listing" 
      WHERE device_id IN ('4a770164-0392-4a41-8565-40ed8cec737e');
    `);

    const row = result.getRows()[0];
    expect(row[0]).toBe(deviceIds[0]);
  });

  it.skip("should require partition filters for injected projection", async () => {
    glueMock.on(GetTableCommand).resolves({
      Table: {
        Name: "injected_projection",
        DatabaseName: "test_db",
        PartitionKeys: [{ Name: "device_id", Type: "string" }],
        StorageDescriptor: {
          Location: "s3://test-bucket/device-data/",
        },
        Parameters: {
          "projection.enabled": "true",
          "projection.device_id.type": "injected",
          "storage.location.template": "s3://test-bucket/device-data/${device_id}",
        },
      },
    });

    await expect(async () => {
      await cache.getFilteredS3Locations("test_db", "injected_projection", []);
    }).rejects.toThrow(
      "For the injected projected partition column device_id, the WHERE clause must contain only static equality conditions"
    );
  });

  it.skip("should handle missing injected values", async () => {
    glueMock.on(GetTableCommand).resolves({
      Table: {
        Name: "missing_injection",
        DatabaseName: "test_db",
        PartitionKeys: [{ Name: "custom", Type: "string" }],
        StorageDescriptor: {
          Location: "s3://test-bucket/missing-data/",
        },
        Parameters: {
          "projection.enabled": "true",
          "projection.custom.type": "injected",
        },
      },
    });

    await expect(async () => {
      await cache.getFilteredS3Locations("test_db", "missing_injection", []);
    }).rejects.toThrow("No injected values found for partition key custom");
  });

  it("should handle missing table response", async () => {
    glueMock.on(GetTableCommand).resolves({});

    await expect(async () => {
      await cache.getTableMetadata("test_db", "missing_table");
    }).rejects.toThrow("Table test_db.missing_table not found");
  });

  it("should handle JSON array format in projection range", async () => {
    glueMock.on(GetTableCommand).resolves({
      Table: {
        Name: "json_range",
        DatabaseName: "test_db",
        PartitionKeys: [{ Name: "year", Type: "string" }],
        StorageDescriptor: {
          Location: "s3://test-bucket/json-range/",
        },
        Parameters: {
          "projection.enabled": "true",
          "projection.year.type": "enum",
          "projection.year.range": "[2020,2021,2022]",
        },
      },
    });

    const metadata = await cache.getTableMetadata("test_db", "json_range");
    expect(metadata.projectionPatterns?.patterns.year.range).toEqual([2020, 2021, 2022]);
  });

  it("should handle comma-separated format in projection range", async () => {
    glueMock.on(GetTableCommand).resolves({
      Table: {
        Name: "csv_range",
        DatabaseName: "test_db",
        PartitionKeys: [{ Name: "year", Type: "string" }],
        StorageDescriptor: {
          Location: "s3://test-bucket/csv-range/",
        },
        Parameters: {
          "projection.enabled": "true",
          "projection.year.type": "enum",
          "projection.year.range": "2020,2021,2022",
        },
      },
    });

    const metadata = await cache.getTableMetadata("test_db", "csv_range");
    expect(metadata.projectionPatterns?.patterns.year.range).toEqual(["2020", "2021", "2022"]);
  });

  it("should throw error for unsupported projection types", async () => {
    glueMock.on(GetTableCommand).resolves({
      Table: {
        Name: "unsupported_projection",
        DatabaseName: "test_db",
        PartitionKeys: [{ Name: "custom", Type: "string" }],
        StorageDescriptor: {
          Location: "s3://test-bucket/unsupported-data/",
        },
        Parameters: {
          "projection.enabled": "true",
          "projection.custom.type": "unsupported_type",
        },
      },
    });

    const testFiles = ["s3://test-bucket/unsupported-data/custom_value/data.parquet"];

    const s3Mock = jest.spyOn(cache as any, "listS3Files");
    s3Mock.mockResolvedValue(
      testFiles.map((path) => ({
        path,
        partitionValues: {},
      }))
    );

    await expect(async () => {
      await cache.getFilteredS3Locations("test_db", "unsupported_projection", []);
    }).rejects.toThrow("Unsupported projection type: unsupported_type");
  });
});
