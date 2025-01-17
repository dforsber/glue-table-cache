/* eslint-disable @typescript-eslint/no-explicit-any */
import { GlueTableCache, type S3FileInfo } from "../src/glue-table-cache";

describe("GlueTableCache Integration Tests", () => {
  const cache = new GlueTableCache({
    glueTableMetadataTtlMs: 3600000,
    maxEntries: 100,
    forceRefreshOnError: true,
    s3ListingRefreshMs: 60000,
  });

  const database = "default";
  const tables = ["iceberg_table", "flight_delays_pq", "flights_parquet"];

  beforeAll(() => {
    // Ensure AWS credentials are available
    if (!process.env.AWS_REGION && !process.env.AWS_PROFILE) {
      console.warn("Warning: AWS credentials should be configured");
    }
  });

  it("should fetch metadata for all test tables", async () => {
    for (const tableName of tables) {
      const metadata = await cache.getTableMetadata(database, tableName);
      expect(metadata.table.Name).toBe(tableName);
      expect(metadata.table.DatabaseName).toBe(database);
      expect(metadata.timestamp).toBeDefined();
      expect(metadata.timestamp).toBeLessThanOrEqual(Date.now());
    }
  });

  it("should cache table metadata and reduce API calls", async () => {
    // First call - should hit AWS API
    const metadata1 = await cache.getTableMetadata(database, tables[0]);
    const timestamp1 = metadata1.timestamp;

    // Second call - should use cache
    const metadata2 = await cache.getTableMetadata(database, tables[0]);
    const timestamp2 = metadata2.timestamp;

    expect(timestamp1).toBe(timestamp2);
    expect(metadata1).toEqual(metadata2);
  });

  it("should handle partition information correctly", async () => {
    for (const tableName of tables) {
      const metadata = await cache.getTableMetadata(database, tableName);

      if (metadata.table.PartitionKeys && metadata.table.PartitionKeys.length > 0) {
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
    const metadata1 = await cache.getTableMetadata(database, tableName);

    // Invalidate cache
    cache.invalidateTable(database, tableName);

    // Second fetch should get fresh data
    const metadata2 = await cache.getTableMetadata(database, tableName);

    expect(metadata2.timestamp).toBeGreaterThan(metadata1.timestamp);
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
      "CREATE OR REPLACE TABLE \"default.flights_parquet_s3_files\" AS SELECT path FROM (VALUES ('s3://athena-examples-eu-west-1/flight/parquet/year=1987/000008_0'),('s3://athena-examples-eu-west-1/flight/parquet/year=1988/000006_0'),('s3://athena-examples-eu-west-1/flight/parquet/year=1989/000005_0'),('s3://athena-examples-eu-west-1/flight/parquet/year=1990/000002_0'),('s3://athena-examples-eu-west-1/flight/parquet/year=1991/000008_0'),('s3://athena-examples-eu-west-1/flight/parquet/year=1992/000006_0'),('s3://athena-examples-eu-west-1/flight/parquet/year=1993/000000_0'),('s3://athena-examples-eu-west-1/flight/parquet/year=1994/000000_0'),('s3://athena-examples-eu-west-1/flight/parquet/year=1995/000004_0'),('s3://athena-examples-eu-west-1/flight/parquet/year=1996/000013_0'),('s3://athena-examples-eu-west-1/flight/parquet/year=1997/000011_0'),('s3://athena-examples-eu-west-1/flight/parquet/year=1998/000005_0'),('s3://athena-examples-eu-west-1/flight/parquet/year=1999/000011_0'),('s3://athena-examples-eu-west-1/flight/parquet/year=2000/000013_0'),('s3://athena-examples-eu-west-1/flight/parquet/year=2001/000002_0'),('s3://athena-examples-eu-west-1/flight/parquet/year=2002/000002_0'),('s3://athena-examples-eu-west-1/flight/parquet/year=2003/000009_0'),('s3://athena-examples-eu-west-1/flight/parquet/year=2004/000010_0'),('s3://athena-examples-eu-west-1/flight/parquet/year=2005/000005_0'),('s3://athena-examples-eu-west-1/flight/parquet/year=2006/000006_0'),('s3://athena-examples-eu-west-1/flight/parquet/year=2007/000000_0'),('s3://athena-examples-eu-west-1/flight/parquet/year=2008/000005_0'),('s3://athena-examples-eu-west-1/flight/parquet/year=2009/000009_0'),('s3://athena-examples-eu-west-1/flight/parquet/year=2010/000012_0'),('s3://athena-examples-eu-west-1/flight/parquet/year=2011/000007_0'),('s3://athena-examples-eu-west-1/flight/parquet/year=2012/000013_0'),('s3://athena-examples-eu-west-1/flight/parquet/year=2013/000011_0'),('s3://athena-examples-eu-west-1/flight/parquet/year=2014/000005_0'),('s3://athena-examples-eu-west-1/flight/parquet/year=2015/000003_0'),('s3://athena-examples-eu-west-1/flight/parquet/year=2016/000012_0')) t(path);CREATE OR REPLACE TABLE \"default.flights_parquet_s3_listing\" AS SELECT path, regexp_extract(path, 'year=([^/]+)', 1) as year FROM \"default.flights_parquet_s3_files\";CREATE INDEX IF NOT EXISTS idx_year ON \"default.flights_parquet_s3_listing\" (year);SET VARIABLE default_flights_parquet_files = (SELECT list(path) FROM \"default.flights_parquet_s3_listing\");SET VARIABLE default_flights_parquet_gview_files = ( SELECT list(path) FROM (SELECT path FROM \"default.flights_parquet_s3_listing\"));CREATE OR REPLACE VIEW default_flights_parquet_gview AS SELECT * FROM parquet_scan(getvariable('default_flights_parquet_gview_files'));SELECT * FROM parquet_scan(getvariable('default_flights_parquet_files')) LIMIT 10;"
    );

    // Execute the converted query
    const result = await (cache as any)?.runAndReadAll(convertedQuery);
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
    expect(convertedComplexQuery).not.toContain("glue.default.flights_parquet");

    const complexResult = await (cache as any)?.runAndReadAll(convertedComplexQuery);
    const complexRows = complexResult.getRows();

    expect(complexRows.length).toBe(5);
    expect(complexRows[0].length).toBeGreaterThan(0); // Should have multiple columns
    expect(complexRows[0]).toBeDefined();
  }, 30_000);

  it("should handle partition projection patterns", async () => {
    const cache = new GlueTableCache();
    
    // Test date format projection
    const datePattern = await (cache as any).getPartitionExtractor("dt", {
      projectionPatterns: {
        enabled: true,
        patterns: {
          dt: {
            type: "date",
            format: "yyyy-MM-dd"
          }
        }
      }
    });
    expect(datePattern).toContain("regexp_extract");
    expect(datePattern).toContain("\\d{4}-\\d{2}-\\d{2}");

    // Test integer projection
    const intPattern = await (cache as any).getPartitionExtractor("year", {
      projectionPatterns: {
        enabled: true,
        patterns: {
          year: {
            type: "integer"
          }
        }
      }
    });
    expect(intPattern).toContain("CAST");
    expect(intPattern).toContain("INTEGER");

    // Test enum projection
    const enumPattern = await (cache as any).getPartitionExtractor("category", {
      projectionPatterns: {
        enabled: true,
        patterns: {
          category: {
            type: "enum"
          }
        }
      }
    });
    expect(enumPattern).toContain("regexp_extract");
    expect(enumPattern).toContain("[^/]+");

    // Test default Hive-style partitioning
    const hivePattern = await (cache as any).getPartitionExtractor("partition_col", {
      projectionPatterns: { enabled: false }
    });
    expect(hivePattern).toContain("partition_col=([^/]+)");
  }, 30_000);

  it("should list and filter S3 files from athena-examples", async () => {
    const cache = new GlueTableCache();

    // Use the actual S3 path
    const s3Files = await (cache as any).listS3Files(
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
