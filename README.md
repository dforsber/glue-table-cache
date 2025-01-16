# Map Glue Table to DuckDB VIEW using S3 Listing with LRU Caching

A TypeScript library that provides an LRU cache layer for AWS Glue Table metadata and S3 listings, optimizing access patterns and reducing API calls to AWS Glue and S3. It features DuckDB integration for efficient SQL-based partition filtering and supports both standard Hive-style partitioned tables and AWS Glue partition projection patterns.

Convert SQL queries reading from Glue Tables to DuckDB Parquet scanning over pruned S3 listings.

```sql
SELECT * FROM glue.db.tabl;
-- Convert to reading directly from S3 with partition pruned listing
SELECT * FROM parquet_scan(['s3://../file1.parquet', ..., 's3://../fileN.parquet'])
```

## Features

- 🚀 Convert SQL query reading Glue Table to S3 read query with partition pruning
- 🔍 SQL-based partition filtering using DuckDB
  - 📊 Support for Hive-style partitioned tables
  - 🎯 Support for AWS Glue partition projection patterns:
    - Date-based projections
    - Integer range projections
    - Enum value projections
- 📂 Efficient S3 file listing with caching
- 🚀 LRU (Least Recently Used) caching mechanism for Glue metadata
  - ⏰ Configurable TTL for cache entries
  - 🔄 Automatic cache invalidation and refresh
- 🔒 Type-safe TypeScript implementation

## Installation

```bash
npm install glue-table-cache
# or
yarn add glue-table-cache
```

## Usage

### Converting Glue Table SQL Queries

```typescript
import { GlueTableCache } from "glue-table-cache";

// Initialize the cache
const cache = new GlueTableCache("eu-west-1");

// Example: Convert a complex Glue Table query into DuckDB SQL statements
const database = "mydatabase";
const tableName = "mytable";
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
const statements = await cache.getGlueTableViewSetupSql(database, tableName, query);

// Execute each statement to set up the view
for (const sql of statements) {
  await db.run(sql);
}

// The statements will:
// 1. Create table for S3 file paths
// 2. Create table for partition listings with extractors
// 3. Create indexes on partition columns
// 4. Set variable with file list
// 5. Create the final view that transforms the original query

// Now you can query the view directly
const result = await db.run(`SELECT * FROM ${viewName}`);
```

### Basic Setup

```typescript
import { GlueTableCache } from "glue-table-cache";

const cache = new GlueTableCache("eu-west-1", {
  ttlMs: 3600000, // Cache TTL: 1 hour
  maxEntries: 100, // Maximum number of tables to cache
  forceRefreshOnError: true, // Invalidate cache on errors
  s3ListingRefreshMs: 300000, // S3 listing cache TTL: 5 minutes
});
```

### Working with Table Metadata

```typescript
// Get table metadata with automatic caching
const metadata = await cache.getTableMetadata("mydatabase", "mytable");

// Access table properties
console.log(metadata.table.Name);
console.log(metadata.table.StorageDescriptor?.Location);

// Check partition information
if (metadata.projectionPatterns?.enabled) {
  // Handle projection-enabled table
  console.log("Projection patterns:", metadata.projectionPatterns.patterns);
} else if (metadata.partitionMetadata) {
  // Handle standard partitioned table
  console.log("Partition keys:", metadata.partitionMetadata.keys);
}

// Generate complete SQL setup for a view
const statements = await cache.getGlueTableViewSetupSql(
  "mydatabase",
  "mytable",
  "SELECT * FROM glue.mydatabase.mytable"
);

// Execute all setup statements
await db.run(sql.join(";\n"));
```

### SQL-Based File Filtering

```typescript
// Get filtered S3 locations using SQL predicates
const locations = await cache.getFilteredS3Locations("mydatabase", "mytable", [
  "year >= '2023'",
  "month IN ('01', '02', '03')",
  "region = 'eu-west-1'",
]);

// Convert Glue table references in SQL queries
const query = `
  WITH monthly_stats AS (
    SELECT year, month,
           COUNT(*) as flights,
           AVG(delay) as avg_delay
    FROM glue.mydatabase.mytable
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
`;

// The query above gets converted to use parquet_scan:
const convertedQuery = await cache.convertGlueTableQuery(query);
console.log(convertedQuery);
/* Output:
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
```

### Cache Management

```typescript
// Clear entire cache
cache.clearCache();
// Invalidate specific table
cache.invalidateTable("mydatabase", "mytable");
// Clean up resources
await cache.close();
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

- `convertGlueTableQuery(query: string): Promise<string>`
  - Converts Glue table references to DuckDB parquet_scan operations
- `getGlueTableViewSetupSql(database: string, tableName: string, query: string): Promise<string[]>`
  - Generates complete SQL setup for creating a DuckDB view over a Glue table
  - Returns array of SQL statements that:
    1. Create table for S3 file paths
    2. Create table for partition listings with extractors
    3. Create indexes on partition columns
    4. Set variable with file list
    5. Create the final view

#### Metadata Operations

- `getTableMetadata(database: string, tableName: string): Promise<CachedTableMetadata>`
  - Retrieves Glue Table metadata with caching
- `clearCache(): void`
  - Clears all cached metadata
- `invalidateTable(database: string, tableName: string): void`
  - Invalidates cache for specific table

#### File Operations

- `getS3Locations(database: string, tableName: string, filter?: PartitionFilter): Promise<string[]>`
  - Gets all matching S3 file locations
- `getFilteredS3Locations(database: string, tableName: string, filters: string[]): Promise<string[]>`
  - Gets S3 locations filtered by SQL predicates

## Performance Features

- ⚡️ LRU caching reduces AWS API calls
- 📊 DuckDB for efficient SQL operations
- 🔍 Automatic index creation for partition columns
- 🗂 S3 file listing cache with configurable TTL
- 🔄 Automatic cache invalidation on errors
- 📈 Partition value extraction from S3 paths

## Requirements

- Node.js >= 16.0.0
- AWS credentials with Glue and S3 permissions
- DuckDB-compatible system architecture
