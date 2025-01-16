import { DuckDBInstance } from "@duckdb/node-api";
import * as fs from "fs";
import * as path from "path";

async function generateAst() {
  const db = await (await DuckDBInstance.create(":memory:")).connect();
  const fixtures: Record<string, { sql: string; ast: any }> = {};

  // Add more test cases here
  const testCases: Record<string, string> = {
    join_query: `
      SELECT t1.col1, t2.col2 
      FROM glue.db1.table1 t1 
      JOIN glue.db2.table2 t2 ON t1.id = t2.id
    `,
    subquery: `
      SELECT * FROM (
        SELECT col1, col2 
        FROM glue.mydb.mytable 
        WHERE col1 > 0
      ) t
    `,
    complex_cte: `
      WITH monthly_stats AS (
        SELECT year, month, COUNT(*) as flights
        FROM glue.mydb.mytable
        WHERE year = '2024' AND month IN ('01', '02')
        GROUP BY year, month
      )
      SELECT year, SUM(flights) as total_flights
      FROM monthly_stats
      GROUP BY year
      ORDER BY year DESC
    `,
    parq: `SELECT * FROM parquet_scan(getvariable('mydb_mytable_files'))`,
  };

  for (const [name, sql] of Object.entries(testCases)) {
    const result = await db.runAndReadAll(
      `SELECT json_serialize_sql('${sql.replace(/'/g, "''")}')`
    );
    const rows = result.getRows();
    fixtures[name] = {
      sql: sql.trim(),
      ast: JSON.parse(rows[0][0] as string),
    };
  }

  const fixturePath = path.join(__dirname, "duckdb-ast.json");
  fs.writeFileSync(fixturePath, JSON.stringify(fixtures, null, 2));

  await db.close();
}

generateAst().catch(console.error);
