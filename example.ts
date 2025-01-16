import { GlueTableCache } from "./dist/index.js";
import { DuckDBInstance } from "@duckdb/node-api";

const cache = new GlueTableCache();
const query = `SELECT * FROM glue.default.flights_parquet WHERE year = '2016' AND month IN ('01', '02', '03') LIMIT 100;`;

// Convert the query into DuckDB SQL using the parquet scan format.
const duckQuery = await cache.convertGlueTableQuery(query);

// When executing, you need to have AWS S3 credentials set up:
console.log(duckQuery);

const db = await (await DuckDBInstance.create(":memory:")).connect();
const secSql = "CREATE SECRET secretForDirectS3Access ( TYPE S3, PROVIDER CREDENTIAL_CHAIN );";
const s = duckQuery.split(";").filter(Boolean);
const convertedQuery = <string>s.pop();
await db.run(secSql + s.join(";"));

// Run the actual query by streaming 10 rows at a time
console.log(convertedQuery);
let rows = 10;
const resultsStream = await db.streamAndRead(convertedQuery);
while (!resultsStream.done) {
  await resultsStream.readUntil(rows);
  console.log(resultsStream.getRows());
  rows += 10;
}
