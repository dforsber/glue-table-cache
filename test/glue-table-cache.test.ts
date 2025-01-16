/* eslint-disable @typescript-eslint/no-explicit-any */
import { GetPartitionsCommand, GetTableCommand, GlueClient } from "@aws-sdk/client-glue";
import { GlueTableCache } from "../src/glue-table-cache";
import { mockClient } from "aws-sdk-client-mock";

const glueMock = mockClient(GlueClient);

describe("GlueTableCache", () => {
  let cache: GlueTableCache;

  beforeEach(() => {
    glueMock.reset();
    cache = new GlueTableCache();
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

    const cache = new GlueTableCache({
      glueTableMetadataTtlMs: 3600000,
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

    const cache = new GlueTableCache({
      glueTableMetadataTtlMs: 3600000,
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

    const cache = new GlueTableCache({
      glueTableMetadataTtlMs: 3600000,
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

    const cache = new GlueTableCache({
      glueTableMetadataTtlMs: 100, // Short TTL for testing
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

    const cache = new GlueTableCache();

    // Mock S3 listing to return some test files
    const s3Mock = jest.spyOn(cache as any, "listS3Files");
    s3Mock.mockResolvedValue([
      {
        path: "s3://test-bucket/mydb/mytable/year=2024/month=01/data.parquet",
        partitionValues: { year: "2024", month: "01" },
      },
    ]);

    const statements = await (cache as any)?.getGlueTableViewSetupSql(
      "SELECT * FROM glue.mydb.mytable"
    );

    expect(statements).toHaveLength(7); // Base table, listing table, index, variable, view
    expect(statements[0]).toContain('CREATE OR REPLACE TABLE "mydb.mytable_s3_files"');
    expect(statements[1]).toContain('CREATE OR REPLACE TABLE "mydb.mytable_s3_listing"');
    expect(statements[2]).toContain("CREATE INDEX");
    expect(statements[4]).toContain("SET VARIABLE mydb_mytable_files");
    expect(statements[5]).toContain("SET VARIABLE mydb_mytable_gview_files");
    expect(statements[6]).toContain("CREATE OR REPLACE VIEW mydb_mytable_gview");
  });
});

describe("GlueTableCache Partition Extraction", () => {
  let cache: GlueTableCache;

  beforeEach(async () => {
    glueMock.reset();
    cache = new GlueTableCache();
  });

  afterEach(async () => {
    await cache.close();
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

  it("should handle error cases", async () => {
    glueMock.on(GetTableCommand).rejects(new Error("AWS Error"));

    const cache = new GlueTableCache({
      forceRefreshOnError: true,
    });

    await expect(cache.getTableMetadata("test_db", "error_table")).rejects.toThrow("AWS Error");
    expect(glueMock.calls().length).toBe(1);
  });

  it("should parse S3 paths correctly", async () => {
    const cache = new GlueTableCache({
      glueTableMetadataTtlMs: 3600000,
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
});
