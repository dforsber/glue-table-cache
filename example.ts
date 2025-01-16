import { GlueTableCache } from "./dist/index.js";

const cache = new GlueTableCache();
const query = `SELECT * FROM glue.default.flights_parquet WHERE year = '2016' AND month IN ('01', '02', '03') LIMIT 10;`;

// Convert the query into DuckDB SQL using the parquet scan format.
const duckQuery = await cache.convertGlueTableQuery(query);

// When executing, you need to have AWS S3 credentials set up:
// -- CREATE SECRET secretForDirectS3Access ( TYPE S3,  PROVIDER CREDENTIAL_CHAIN );
console.log(duckQuery);
