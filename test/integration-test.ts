/* eslint-disable @typescript-eslint/no-explicit-any */
import { S3Client } from "@aws-sdk/client-s3";
import { GlueTableCache, type S3FileInfo } from "../src/glue-table-cache";
import { listS3Objects } from "../src/util/s3";
import { ETableType } from "../src/types";
import { fromIni } from "@aws-sdk/credential-providers";

const database = "default";
const tables = ["iceberg_table", "flight_delays_pq", "flights_parquet"];
let cache: GlueTableCache;

describe("GlueTableCache with no AWS credentials provided", () => {
  it("can also use the default credentials provider chain", async () => {
    const cache = new GlueTableCache({
      glueTableMetadataTtlMs: 3600000,
      maxEntries: 100,
      s3ListingRefresTtlhMs: 60000,
    });
    const tableName = "iceberg_table";
    const metadata = await cache.getTableMetadataCached(database, tableName);
    expect(metadata?.table.Name).toBe(tableName);
    expect(metadata?.table.DatabaseName).toBe(database);
    expect(metadata?.timestamp).toBeDefined();
    expect(metadata?.timestamp).toBeLessThanOrEqual(Date.now());
  });
});

describe("GlueTableCache Integration Tests", () => {
  beforeAll(async () => {
    const credentials = await fromIni()();
    cache = new GlueTableCache({
      glueTableMetadataTtlMs: 3600000,
      maxEntries: 100,
      s3ListingRefresTtlhMs: 60000,
      credentials,
    });
  });

  it("should not retry on persistent errors", async () => {
    try {
      await cache.getTableMetadataCached(database, "nonexisting");
    } catch (error: any) {
      console.error(error);
      expect(error?.message).toContain("Entity Not Found");
    }
  });

  it("should fetch metadata for all test tables", async () => {
    for (const tableName of tables) {
      const metadata = await cache.getTableMetadataCached(database, tableName);
      expect(metadata?.table.Name).toBe(tableName);
      expect(metadata?.table.DatabaseName).toBe(database);
      expect(metadata?.timestamp).toBeDefined();
      expect(metadata?.timestamp).toBeLessThanOrEqual(Date.now());
    }
  });

  it("should cache table metadata and reduce API calls", async () => {
    // First call - should hit AWS API
    const metadata1 = await cache.getTableMetadataCached(database, tables[0]);
    const timestamp1 = metadata1?.timestamp;

    // Second call - should use cache
    const metadata2 = await cache.getTableMetadataCached(database, tables[0]);
    const timestamp2 = metadata2?.timestamp;

    expect(timestamp1).toBe(timestamp2);
    expect(metadata1).toEqual(metadata2);
  });

  it("should handle partition information correctly", async () => {
    for (const tableName of tables) {
      const metadata = await cache.getTableMetadataCached(database, tableName);

      if (metadata?.table.PartitionKeys && metadata?.table.PartitionKeys.length > 0) {
        if (metadata.projectionPatterns?.enabled) {
          // Check projection patterns
          expect(metadata.projectionPatterns.patterns).toBeDefined();
          expect(Object.keys(metadata.projectionPatterns.patterns).length).toBeGreaterThan(0);
        } else {
          // Check standard partitions
          expect(metadata.partitionMetadata).toBeDefined();
          if (metadata.partitionMetadata) {
            expect(metadata.partitionMetadata.keys.length).toBeGreaterThan(0);
            expect(metadata.partitionMetadata.values.length).toBeGreaterThan(0);
          }
        }
      }
    }
  });

  it("should handle cache invalidation correctly", async () => {
    const tableName = tables[0];

    // First fetch
    const metadata1 = await cache.getTableMetadataCached(database, tableName);

    // Invalidate cache
    cache.invalidateTable(database, tableName);

    // Second fetch should get fresh data
    const metadata2 = await cache.getTableMetadataCached(database, tableName);

    expect(metadata1).toBeDefined();
    expect(metadata2).toBeDefined();
    if (!metadata1?.timestamp) throw new Error("test fails");
    expect(metadata2?.timestamp).toBeGreaterThan(metadata1?.timestamp);
  });
});

describe("GlueTableCache", () => {
  it("should convert and execute Glue table query", async () => {
    const cache = new GlueTableCache();

    // First get the table metadata and ensure S3 listing exists
    await cache.createGlueTableFilesVarSql("default", "flights_parquet");

    const query = "SELECT * FROM glue.default.flights_parquet LIMIT 10;";
    const convertedQuery = await cache.convertGlueTableQuery(query);

    // Verify the SQL conversion
    expect(convertedQuery).toBe(
      "CREATE OR REPLACE TABLE \"default_flights_parquet_s3_files\" AS SELECT path FROM (VALUES ('s3://athena-examples-eu-west-1/flight/parquet/year=1987/000008_0'),('s3://athena-examples-eu-west-1/flight/parquet/year=1988/000006_0'),('s3://athena-examples-eu-west-1/flight/parquet/year=1989/000005_0'),('s3://athena-examples-eu-west-1/flight/parquet/year=1990/000002_0'),('s3://athena-examples-eu-west-1/flight/parquet/year=1991/000008_0'),('s3://athena-examples-eu-west-1/flight/parquet/year=1992/000006_0'),('s3://athena-examples-eu-west-1/flight/parquet/year=1993/000000_0'),('s3://athena-examples-eu-west-1/flight/parquet/year=1994/000000_0'),('s3://athena-examples-eu-west-1/flight/parquet/year=1995/000004_0'),('s3://athena-examples-eu-west-1/flight/parquet/year=1996/000013_0'),('s3://athena-examples-eu-west-1/flight/parquet/year=1997/000011_0'),('s3://athena-examples-eu-west-1/flight/parquet/year=1998/000005_0'),('s3://athena-examples-eu-west-1/flight/parquet/year=1999/000011_0'),('s3://athena-examples-eu-west-1/flight/parquet/year=2000/000013_0'),('s3://athena-examples-eu-west-1/flight/parquet/year=2001/000002_0'),('s3://athena-examples-eu-west-1/flight/parquet/year=2002/000002_0'),('s3://athena-examples-eu-west-1/flight/parquet/year=2003/000009_0'),('s3://athena-examples-eu-west-1/flight/parquet/year=2004/000010_0'),('s3://athena-examples-eu-west-1/flight/parquet/year=2005/000005_0'),('s3://athena-examples-eu-west-1/flight/parquet/year=2006/000006_0'),('s3://athena-examples-eu-west-1/flight/parquet/year=2007/000000_0'),('s3://athena-examples-eu-west-1/flight/parquet/year=2008/000005_0'),('s3://athena-examples-eu-west-1/flight/parquet/year=2009/000009_0'),('s3://athena-examples-eu-west-1/flight/parquet/year=2010/000012_0'),('s3://athena-examples-eu-west-1/flight/parquet/year=2011/000007_0'),('s3://athena-examples-eu-west-1/flight/parquet/year=2012/000013_0'),('s3://athena-examples-eu-west-1/flight/parquet/year=2013/000011_0'),('s3://athena-examples-eu-west-1/flight/parquet/year=2014/000005_0'),('s3://athena-examples-eu-west-1/flight/parquet/year=2015/000003_0'),('s3://athena-examples-eu-west-1/flight/parquet/year=2016/000012_0')) t(path);CREATE OR REPLACE TABLE \"default_flights_parquet_s3_listing\" AS SELECT path, regexp_extract(path, 'year=([^/]+)', 1) as year FROM \"default_flights_parquet_s3_files\";CREATE INDEX IF NOT EXISTS idx_year ON \"default_flights_parquet_s3_listing\" (year);SET VARIABLE default_flights_parquet_files = (SELECT list(path) FROM \"default_flights_parquet_s3_listing\");SET VARIABLE default_flights_parquet_gview_files = ( SELECT list(path) FROM (SELECT path FROM \"default_flights_parquet_s3_listing\"));CREATE OR REPLACE VIEW GLUE__default_flights_parquet AS SELECT * FROM parquet_scan(getvariable('default_flights_parquet_gview_files'));SELECT * FROM parquet_scan(getvariable('default_flights_parquet_files')) LIMIT 10;"
    );

    // Execute the converted query
    const result = await (cache as any)?.__runAndReadAll(convertedQuery);
    const rows = result.getRows();

    expect(rows).toBeDefined();
    expect(Array.isArray(rows)).toBe(true);
    expect(rows.length).toBe(10);
    expect(rows[0].length).toBeGreaterThan(0); // Should have columns

    // Verify the query works with more complex SQL
    const complexQuery = `
      WITH monthly_stats AS (
        SELECT
          year,
          month,
          COUNT(*) as flight_count,
          AVG(depdelay) as avg_delay,
          COUNT(CASE WHEN depdelay > 15 THEN 1 END) as delayed_flights
        FROM glue.default.flights_parquet
        WHERE CAST(year AS INTEGER) >= 2000 
          AND month IN ('01', '02')
          AND origin IS NOT NULL
        GROUP BY year, month
      ),
      yearly_summary AS (
        SELECT
          year,
          SUM(flight_count) as total_flights,
          AVG(avg_delay) as yearly_avg_delay,
          SUM(delayed_flights) as total_delays
        FROM monthly_stats
        GROUP BY year
      )
      SELECT 
        m.*,
        y.total_flights,
        y.yearly_avg_delay,
        (m.delayed_flights::FLOAT / m.flight_count) * 100 as delay_percentage
      FROM monthly_stats m
      JOIN yearly_summary y ON m.year = y.year
      WHERE m.avg_delay > 10
      ORDER BY m.year DESC, m.month ASC
      LIMIT 5;
    `;

    const convertedComplexQuery = await cache.convertGlueTableQuery(complexQuery);
    expect(convertedComplexQuery).toContain(
      "parquet_scan(getvariable('default_flights_parquet_files'))"
    );
    expect(convertedComplexQuery).not.toContain("glue.default_flights_parquet");

    const complexResult = await (cache as any)?.__runAndReadAll(convertedComplexQuery);
    const complexRows = complexResult.getRows();

    expect(complexRows.length).toBe(5);
    expect(complexRows[0].length).toBeGreaterThan(0); // Should have multiple columns
    expect(complexRows[0]).toBeDefined();
  }, 30_000);

  it("should verify Iceberg table S3 listing contains only data files", async () => {
    const cache = new GlueTableCache();
    const database = "default";
    const tableName = "iceberg_table";

    // Get the table metadata which includes the S3 location
    const metadata = await cache.getTableMetadataCached(database, tableName);
    expect(metadata?.tableType).toBe(ETableType.ICEBERG);
    expect(metadata?.table.StorageDescriptor?.Location).toBeDefined();

    // Get the SQL statements that set up the table
    const secrets = `CREATE SECRET secretForDirectS3Access ( TYPE S3, PROVIDER CREDENTIAL_CHAIN );`;
    await (cache as any).__runAndReadAll(secrets);
    const query = `SELECT * FROM glue.${database}.${tableName}`;
    const statements = await cache.getGlueTableViewSetupSql(query);

    // Find the statement that creates the s3_files table
    const s3FilesStmt = statements.find((stmt) =>
      stmt.includes(`CREATE OR REPLACE TABLE "${database}_${tableName}_s3_files"`)
    );
    expect(s3FilesStmt).toBeDefined();

    // Verify no manifest or metadata files are included
    expect(s3FilesStmt).not.toContain("manifest.json");
    expect(s3FilesStmt).not.toContain("metadata.json");
    expect(s3FilesStmt).toMatch(/\.parquet/); // Should contain parquet files
  }, 30_000);

  it("should list and filter S3 files from athena-examples", async () => {
    // Use the actual S3 path
    const s3cli = new S3Client();
    const s3Files = await listS3Objects(
      s3cli,
      "s3://athena-examples-eu-west-1/flight/parquet/",
      []
    );

    expect(s3Files).toBeDefined();
    expect(Array.isArray(s3Files)).toBe(true);
    expect(s3Files.length).toBeGreaterThan(0);

    // Verify files are valid and don't include folder markers
    s3Files.forEach((file: S3FileInfo) => {
      expect(file.path.includes("_$folder$")).toBe(false);
      // The files might not have .parquet extension but are parquet format
      expect(file.path).toMatch(/s3:\/\/[\w-]+(?:\.[\w-]+)*\/[^\s]+/);
    });
  }, 30_000);
});
