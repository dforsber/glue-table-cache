/* eslint-disable @typescript-eslint/no-explicit-any */
import { GlueTableCache, type S3FileInfo } from "../src/glue-table-cache";

describe("GlueTableCache Integration Tests", () => {
  const cache = new GlueTableCache("eu-west-1", {
    ttlMs: 3600000,
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

describe("GlueTableCache.getFilteredS3Locations", () => {
  it("should support complex partition filtering using DuckDB", async () => {
    const cache = new GlueTableCache("eu-west-1");
    const locations = await cache.getFilteredS3Locations("boilingdata-benchmark", "nyc6trip_data", [
      "year = '2016'",
      "month IN ('01', '02', '03')",
    ]);

    expect(locations).toBeDefined();
    expect(Array.isArray(locations)).toBe(true);
    expect(locations.length).toBe(3); // Should match two premium electronics entries
    expect(locations).toContain(
      "s3://isecurefi-dev-test/nyc-tlc/trip_data/year=2016/month=01/yellow_tripdata_2016-01.parquet"
    );
    expect(locations).toContain(
      "s3://isecurefi-dev-test/nyc-tlc/trip_data/year=2016/month=02/yellow_tripdata_2016-02.parquet"
    );
    expect(locations).toContain(
      "s3://isecurefi-dev-test/nyc-tlc/trip_data/year=2016/month=03/yellow_tripdata_2016-03.parquet"
    );
  }, 30_000);

  it("should convert and execute Glue table query", async () => {
    const cache = new GlueTableCache("eu-west-1");

    // First get the table metadata and ensure S3 listing exists
    const metadata = await cache.getTableMetadata("default", "flights_parquet");
    await (cache as any).ensureS3ListingTable("default", "flights_parquet", metadata);
    await cache.createFileListVariable("default", "flights_parquet");

    const query = "SELECT * FROM glue.default.flights_parquet LIMIT 10;";
    const convertedQuery = await cache.convertGlueTableQuery(query);

    // Verify the SQL conversion
    expect(convertedQuery).toBe(
      "SELECT * FROM parquet_scan(getvariable('default_flights_parquet_files')) LIMIT 10;"
    );

    // Execute the converted query
    const result = await (cache as any)?.runAndReadAll(convertedQuery);
    const rows = result.getRows();

    expect(rows).toBeDefined();
    expect(Array.isArray(rows)).toBe(true);
    expect(rows.length).toBe(10);
    expect(rows[0].length).toBeGreaterThan(0); // Should have columns

    // Ensure table exists for complex query
    await (cache as any).ensureS3ListingTable("default", "flights_parquet", metadata);

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

  it("should list and filter S3 files from athena-examples", async () => {
    const cache = new GlueTableCache("eu-west-1");

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
