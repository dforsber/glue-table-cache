import { stat } from "fs";
import { GlueTableCache } from "./dist/index.js";

// Initialize the cache
const cache = new GlueTableCache("eu-west-1");

// Example: Convert a complex Glue Table query into DuckDB SQL statements
const database = "default";
const tableName = "flights_parquet";
const query = `SELECT * FROM glue.${database}.${tableName} WHERE year = '2016' AND month IN ('01', '02', '03') LIMIT 10;`;

// Get the complete SQL setup statements
const statements = await cache.getGlueTableViewSetupSql(database, tableName, query);
statements.forEach((statement) => console.log(statement + "\n\n"));

// Convert the query into DuckDB SQL using the parquet scan format
// NOTE: This does not use the view itself. It's just a simple conversion.
const duckQuery = await cache.convertGlueTableQuery(query);
console.log(duckQuery);
