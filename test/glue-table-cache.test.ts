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
    cache.close();
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
      s3ListingRefresTtlhMs: 60000,
    });
    await cache.getTableMetadataCached("test_db", "test_table");
    expect(glueMock.calls().length).toBe(1);

    cache.clearCache();
    await cache.getTableMetadataCached("test_db", "test_table");
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
      s3ListingRefresTtlhMs: 60000, // Add this line
    });

    // First call
    await cache.getTableMetadataCached("test_db", "test_table");
    expect(glueMock.calls().length).toBe(1);

    // Wait for TTL to expire
    await new Promise((resolve) => setTimeout(resolve, 150));

    // Second call should refresh
    await cache.getTableMetadataCached("test_db", "test_table");
    expect(glueMock.calls().length).toBe(2);
  });

  it("should handle SQL transformation errors", async () => {
    glueMock.on(GetTableCommand).resolves({
      Table: {
        Name: "test_table",
        DatabaseName: "test_db",
        StorageDescriptor: {
          Location: "s3://test-bucket/data/",
        },
      },
    });

    const s3Mock = jest.spyOn(cache as any, "__listS3FilesCached");
    s3Mock.mockResolvedValue([]);

    // Invalid SQL should throw
    await expect(
      cache.convertGlueTableQuery("INVALID SQL FROM glue.test_db.test_table")
    ).rejects.toThrow();
  });

  it("should handle multiple table references", async () => {
    glueMock.on(GetTableCommand).resolves({
      Table: {
        Name: "test_table",
        DatabaseName: "test_db",
        StorageDescriptor: {
          Location: "s3://test-bucket/data/",
        },
      },
    });

    const s3Mock = jest.spyOn(cache as any, "__listS3FilesCached");
    s3Mock.mockResolvedValue([{ path: "s3://test-bucket/data/file1.parquet" }]);

    const query = `
      SELECT a.col1, b.col2 
      FROM glue.test_db.table1 a
      JOIN glue.test_db.table2 b ON a.id = b.id
    `;

    const convertedQuery = await cache.convertGlueTableQuery(query);

    // Should handle both table references
    expect(convertedQuery).toContain("test_db_table1_files");
    expect(convertedQuery).toContain("test_db_table2_files");
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
    const s3Mock = jest.spyOn(cache as any, "__listS3FilesCached");
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
    expect(statements[0]).toContain('CREATE OR REPLACE TABLE "mydb_mytable_s3_files"');
    expect(statements[1]).toContain('CREATE OR REPLACE TABLE "mydb_mytable_s3_listing"');
    expect(statements[2]).toContain("CREATE INDEX");
    expect(statements[4]).toContain("SET VARIABLE mydb_mytable_files");
    expect(statements[5]).toContain("SET VARIABLE mydb_mytable_gview_files");
    expect(statements[6]).toContain("CREATE OR REPLACE VIEW GLUE__mydb_mytable");
  });

  it("should generate complete view setup SQL when there are not s3 files", async () => {
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
    const s3Mock = jest.spyOn(cache as any, "__listS3FilesCached");
    s3Mock.mockResolvedValue([]);

    const statements = await (cache as any)?.getGlueTableViewSetupSql(
      "SELECT * FROM glue.mydb.mytable"
    );
    expect(statements[6]).toContain(
      "CREATE OR REPLACE VIEW GLUE__mydb_mytable AS SELECT NULL LIMIT 0;"
    );
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

    await cache.getTableMetadataCached("test_db", "test_table");

    const testFiles = [
      "s3://test-bucket/data/file1.parquet",
      "s3://test-bucket/data/file2.parquet",
    ];

    const s3Mock = jest.spyOn(cache as any, "__listS3FilesCached");
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

    await expect(cache.getTableMetadataCached("test_db", "error_table")).rejects.toThrow(
      "AWS Error"
    );
    expect(glueMock.calls().length).toBe(1);
  });

  it("should handle connection lifecycle correctly", async () => {
    const cache = new GlueTableCache();

    // Test initial state
    expect((cache as any).db).toBeUndefined();
    expect((cache as any).sqlTransformer).toBeUndefined();

    // Test connection
    await (cache as any).__connect();
    expect((cache as any).db).toBeDefined();
    expect((cache as any).sqlTransformer).toBeDefined();

    // Test close
    cache.close();
    expect((cache as any).db).toBeUndefined();
    expect((cache as any).sqlTransformer).toBeUndefined();

    // Test reconnection after close
    await (cache as any).__connect();
    expect((cache as any).db).toBeDefined();
    expect((cache as any).sqlTransformer).toBeDefined();
  });

  it("should handle errors in runAndReadAll", async () => {
    const cache = new GlueTableCache();

    // Test with invalid SQL
    await expect((cache as any).__runAndReadAll("INVALID SQL")).rejects.toThrow();

    // Test with unconnected DB
    cache.close();
    await expect((cache as any).__runAndReadAll("SELECT 1")).resolves.toBeDefined();
  });

  it("should handle missing table response", async () => {
    glueMock.on(GetTableCommand).resolves({});

    await expect(async () => {
      await cache.getTableMetadataCached("test_db", "missing_table");
    }).rejects.toThrow("Table test_db.missing_table not found");
  });

  it("should handle proxy address configuration", async () => {
    const cache = new GlueTableCache({
      proxyAddress: "https://localhost:3203",
    });

    glueMock.on(GetTableCommand).resolves({
      Table: {
        Name: "test_table",
        DatabaseName: "test_db",
        StorageDescriptor: {
          Location: "s3://test-bucket/data/",
        },
      },
    });

    const s3Mock = jest.spyOn(cache as any, "__listS3FilesCached");
    s3Mock.mockResolvedValue([{ path: "s3://test-bucket/data/file1.parquet" }]);

    const statements = await (cache as any).getGlueTableViewSetupSql(
      "SELECT * FROM glue.test_db.test_table"
    );

    // Verify proxy address is used in SQL
    expect(statements.some((sql: string) => sql.includes("'https://localhost:3203/"))).toBeTruthy();
  });

  it("should handle invalid proxy address", async () => {
    const cache = new GlueTableCache({
      proxyAddress: "not-a-url",
    });

    // Should fallback to direct S3 paths
    expect((cache as any).config.proxyAddress).toBeUndefined();
  });

  it("should handle missing StorageDescriptor location", async () => {
    glueMock.on(GetTableCommand).resolves({
      Table: {
        Name: "test_table",
        DatabaseName: "test_db",
        StorageDescriptor: {
          // Location missing
        },
      },
    });

    await expect(
      (cache as any).getGlueTableViewSetupSql("SELECT * FROM glue.test_db.test_table")
    ).rejects.toThrow("No storage location found");
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

    const metadata = await cache.getTableMetadataCached("test_db", "json_range");
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

    const metadata = await cache.getTableMetadataCached("test_db", "csv_range");
    expect(metadata.projectionPatterns?.patterns.year.range).toEqual(["2020", "2021", "2022"]);
  });
});
