# Query AWS Glue Tables efficiently with DuckDB

You can use this module to efficiently query AWS Glue Tables from DuckDB while caching all Glue metadata (tables, partitions) and S3 listings.

Both hive partitioning and partition projection based Glue Tables are supported. This module converts SQL query to use e.g. `parquet_scan()` function with explicit partition pruned S3 listings without glob patterns so that DuckDB does not need to start listing the files (objects) on S3, which can be very slow.

DuckDB SQL query AST manipulation is used instead of SQL string matching. Both standard Hive-style partitioned tables and AWS Glue partition projection patterns are supported, (except injected projection for now). Glue Tables are assumed to be Parquet based, but we will support also JSON and CSV based Glue Tables.

> NOTE: This module uses DuckDB itself to do partition pruning by filtering S3 listings stored on DuckDB in-memory Table.

```sql
-- Original unsupport DuckDB SQL query
SELECT * FROM glue.db.tbl;
-- Converts Glue Table to direct S3 read with partition pruned
--   S3 file listing stored on DuckDB variable
SELECT * FROM parquet_scan(getvariable('glue_db_tbl_files'));
```

## Features

- 🚀 Convert SQL query reading Glue Table to direct S3 read query with partition pruning
  - [x] Parquet Glue Tables
  - [ ] JSON/CSV Glue Tables
- 🔍 SQL-based partition filtering using DuckDB
  - 📊 Support for Hive-style partitioned tables
  - 🎯 Support for AWS Glue partition projection patterns tables:
    - [x] Date-based projections
    - [x] Integer range projections
    - [x] Enum value projections
    - [ ] Injected projection from the query
- 🚀 LRU (Least Recently Used) caching mechanism for Glue metadata and S3 listings
  - ⏰ Configurable TTL for cache entries
  - 🔄 Automatic cache invalidation and refresh
- [ ] Allow setting local HTTP proxy block cache for accessing S3 files, so that the s3 URLs are converted to e.g. `http://localhost:3203/BUCKET/PREFIX`
- 🔒 Type-safe TypeScript implementation
- NOTE: DuckDB `json_serialize_sql()` does not support e.g. COPY statements

## Installation

```bash
yarn add glue-table-cache
```

## Usage

### Converting Glue Table SQL Queries

```typescript
import { GlueTableCache } from "glue-table-cache";
import { DuckDBInstance } from "@duckdb/node-api";

// Example: Convert a complex Glue Table query into DuckDB SQL statements
const query = `
  WITH monthly_stats AS (
    SELECT year, month, 
           COUNT(*) as events,
           SUM(amount) as total_amount
    FROM glue.mydatabase.mytable
    WHERE year = '2024' 
      AND month IN ('01', '02', '03')
    GROUP BY year, month
  )
  SELECT year, 
         SUM(events) as total_events,
         AVG(total_amount) as avg_amount
  FROM monthly_stats
  GROUP BY year
  ORDER BY year DESC
`;

// Get the complete SQL setup statements
const cache = new GlueTableCache({
  region: "eu-west-1", // AWS region
  maxEntries: 100, // Maximum number of tables / listings per cache
  forceRefreshOnError: true, // Invalidate cache on errors
  glueTableMetadataTtlMs: 3600000, // Cache TTL: 1 hour
  s3ListingRefreshMs: 3600000, // S3 listing cache TTL: 1 hour
});

// The query above gets converted to use parquet_scan, for each Glue Table reference.
// The returned transformed query includes all SQL statements for creating S3 listing
// table and partition pruned SQL VARIABLE that is then used in the parquet scan.
const convertedQuery = await cache.convertGlueTableQuery(query);
const results = await db.runAndReadAll(convertedQuery);

// The query above gets converted to use parquet_scan:
const convertedQuery = await cache.convertGlueTableQuery(query);
console.log(convertedQuery);
/* Output:

  -- The S3 listing is cached
  CREATE OR REPLACE TABLE "mydatabase.mytable_s3_files" AS 
    SELECT path FROM (VALUES ('s3://...'),('s3://...'),..,(s3://...)) t(path);

  CREATE OR REPLACE TABLE "mydatabase.mytable_s3_listing" AS 
    SELECT path, regexp_extract(path, 'year=([^/]+)', 1) as year 
    FROM "mydatabase.mytable_s3_files";

  CREATE INDEX IF NOT EXISTS idx_year ON "mydatabase.mytable_s3_listing" (year);

  -- This is always query specific because we want to partition prune the files
  SET VARIABLE mydatabase_mytable_files = (
    SELECT list(path) FROM "mydatabase.mytable_s3_listing" 
    WHERE year >= '2023' AND month IN ('01', '02', '03')
  );

  -- This is not query specific
  SET VARIABLE mydatabase_mytable_gview_files = (
    SELECT list(path) FROM "mydatabase.mytable_s3_listing"
  );

  -- There is a view as well, if you happen to check SHOW TABLES, 
  --  but it is query specific!
  CREATE OR REPLACE VIEW GLUE__mydatabase_mytable AS 
    SELECT * FROM parquet_scan(getvariable('default_mytable_gview_files'));

  WITH monthly_stats AS (
    SELECT year, month,
           COUNT(*) as flights,
           AVG(delay) as avg_delay
    FROM parquet_scan(getvariable('mydatabase_mytable_files'))
    WHERE year >= '2023'
      AND month IN ('01', '02', '03')
    GROUP BY year, month
  )
  SELECT year,
         SUM(flights) as total_flights,
         AVG(avg_delay) as yearly_avg_delay
  FROM monthly_stats
  GROUP BY year
  ORDER BY year DESC;
*/

const db = await (await DuckDBInstance.create(":memory:")).connect();
const results = await db.runAndReadAll(convertedQuery);
```

### Cache Management

```typescript
cache.clearCache(); // Clear entire cache
cache.invalidateTable("mydatabase", "mytable"); // Invalidate specific table
await cache.close(); // Clean up resources
```

## API Reference

### Constructor

```typescript
constructor(region: string, config?: CacheConfig)
```

- `region`: AWS region for Glue and S3 clients
- `config`: Optional configuration object
  - `ttlMs`: Metadata cache TTL in milliseconds (default: 1 hour)
  - `maxEntries`: Maximum cache entries (default: 100)
  - `forceRefreshOnError`: Whether to invalidate cache on errors (default: true)
  - `s3ListingRefreshMs`: S3 listing cache TTL in milliseconds (default: 5 minutes)

### Key Methods

#### Map SQL

```typescript
convertGlueTableQuery(query: string): Promise<string>
```

- Converts Glue table references to DuckDB parquet_scan operations

```typescript
getGlueTableViewSetupSql(query: string): Promise<string[]>
```

- Generates complete SQL setup for creating a DuckDB view over a Glue table
- Returns array of SQL statements that:
  1. Create table for S3 file paths
  2. Create table for partition listings with extractors
  3. Create indexes on partition columns
  4. Set variable with file list
  5. Create the final view

#### Metadata Operations

```typescript
getTableMetadata(database: string, tableName: string): Promise<CachedTableMetadata>
```

- Retrieves Glue Table metadata with caching

```typescript
clearCache(): void
```

- Clears all cached metadata

```typescript
invalidateTable(database: string, tableName: string): void
```

- Invalidates cache for specific table

## Performance Features

- ⚡️ LRU caching with configurable TTL reduces AWS API calls
- 📈 Partition value extraction from S3 paths
- 📊 in-memory DuckDB for efficient SQL operations
- 🔍 Automatic index creation for partition columns
- 🔄 Automatic cache invalidation on errors
- 🚀 No slow regexp matching for SQL conversions but converting DuckDB AST
- 🚀 Uses new DuckDB NodeJS (Neo) API module

## Requirements

- Node.js >= 16.0.0
- AWS credentials with Glue and S3 permissions
- DuckDB-compatible system architecture
