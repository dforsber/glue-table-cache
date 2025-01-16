/* eslint-disable @typescript-eslint/no-explicit-any */
import { DuckDBConnection, DuckDBInstance } from "@duckdb/node-api";
import { SqlTransformer } from "../src/sql-transformer.js";
import { JSONPath } from "jsonpath-plus";
import fs from "fs/promises";
import path from "node:path";

describe("SqlTransformer", () => {
  let db: DuckDBConnection;
  let transformer: SqlTransformer;

  beforeAll(async () => {
    db = await (await DuckDBInstance.create(":memory:")).connect();
    transformer = new SqlTransformer(db);
  });

  afterAll(async () => {
    db?.close();
  });

  describe("Basic SQL Transformation", () => {
    it("should verify AST structure and Glue table reference detection", async () => {
      // First get the AST structure
      const result = await db.runAndReadAll(
        `SELECT json_serialize_sql('SELECT * FROM glue.mydb.mytable')`
      );
      const rows = result.getRows();
      const ast = JSON.parse(rows[0][0] as string);

      // Verify AST has expected structure
      expect(ast).toHaveProperty("statements");
      expect(Array.isArray(ast.statements)).toBe(true);
      expect(ast.statements[0]).toHaveProperty("node");
      expect(ast.statements[0].node).toHaveProperty("type", "SELECT_NODE");

      // Find Glue table references using our JSONPath
      const tableRefs = JSONPath({
        json: ast.statements[0],
        path: "$..*[?(@ && @.type == 'BASE_TABLE' && ( @.catalog_name=='glue' || @.catalog_name=='GLUE' ))]",
      });

      // Verify we found the Glue table reference
      expect(tableRefs).toHaveLength(1);
      expect(tableRefs[0]).toMatchObject({
        type: "BASE_TABLE",
        catalog_name: "glue",
        schema_name: "mydb",
        table_name: "mytable",
      });
    });

    it("should transform simple SELECT query", async () => {
      const input = "SELECT * FROM glue.mydb.mytable";
      const output = await transformer.transformGlueTableQuery(input);
      expect(output).toBe("SELECT * FROM parquet_scan(getvariable('mydb_mytable_files'));");
    });

    it("should handle multiple Glue table references", async () => {
      const input = `
        SELECT t1.col1, t2.col2 
        FROM glue.db1.table1 t1 
        JOIN glue.db2.table2 t2 ON t1.id = t2.id
      `;
      const output = await transformer.transformGlueTableQuery(input);
      expect(output).toContain("parquet_scan(getvariable('db1_table1_files'))");
      expect(output).toContain("parquet_scan(getvariable('db2_table2_files'))");
    });

    it("should preserve non-Glue table references", async () => {
      const input = `
        SELECT g.col1, r.col2 
        FROM glue.mydb.mytable g 
        JOIN regular_table r ON g.id = r.id
      `;
      const output = await transformer.transformGlueTableQuery(input);
      expect(output).toContain("parquet_scan(getvariable('mydb_mytable_files'))");
      expect(output).toContain("regular_table");
    });
  });

  describe("Complex Query Handling", () => {
    it("should handle subqueries", async () => {
      const input = `
        SELECT * FROM (
          SELECT col1, col2 
          FROM glue.mydb.mytable 
          WHERE col1 > 0
        ) t
      `;
      const output = await transformer.transformGlueTableQuery(input);
      expect(output).toContain("parquet_scan(getvariable('mydb_mytable_files'))");
      expect(output).toContain("WHERE (col1 > 0)");
    });

    it("should handle CTEs", async () => {
      const input = `
        WITH cte AS (
          SELECT col1, col2 
          FROM glue.mydb.mytable 
          WHERE col1 > 0
        )
        SELECT * FROM cte
      `;
      const output = await transformer.transformGlueTableQuery(input);
      expect(output).toContain("parquet_scan(getvariable('mydb_mytable_files'))");
      expect(output).not.toContain("glue.mydb.mytable");
    });

    it("should handle multiple CTEs", async () => {
      const input = `
        WITH cte1 AS (
          SELECT col1 FROM glue.db1.table1
        ), cte2 AS (
          SELECT col2 FROM glue.db2.table2
        )
        SELECT * FROM cte1 JOIN cte2 ON cte1.col1 = cte2.col2
      `;
      const output = await transformer.transformGlueTableQuery(input);
      expect(output).toContain("parquet_scan(getvariable('db1_table1_files'))");
      expect(output).toContain("parquet_scan(getvariable('db2_table2_files'))");
    });
  });

  describe("SQL Clause Preservation", () => {
    it("should preserve WHERE clauses", async () => {
      const input = "SELECT * FROM glue.mydb.mytable WHERE col1 > 0 AND col2 = 'value'";
      const output = await transformer.transformGlueTableQuery(input);
      expect(output).toContain("WHERE ((col1 > 0) AND (col2 = 'value'))");
    });

    it("should preserve GROUP BY clauses", async () => {
      const input = "SELECT col1, COUNT(*) FROM glue.mydb.mytable GROUP BY col1";
      const output = await transformer.transformGlueTableQuery(input);
      expect(output).toContain("GROUP BY col1");
    });

    it("should preserve ORDER BY clauses", async () => {
      const input = "SELECT * FROM glue.mydb.mytable ORDER BY col1 DESC, col2 ASC";
      const output = await transformer.transformGlueTableQuery(input);
      expect(output).toContain("ORDER BY col1 DESC, col2 ASC");
    });

    it("should preserve LIMIT clauses", async () => {
      const input = "SELECT * FROM glue.mydb.mytable LIMIT 10";
      const output = await transformer.transformGlueTableQuery(input);
      expect(output).toContain("LIMIT 10");
    });
  });

  describe("Error Handling", () => {
    it("should handle invalid SQL gracefully", async () => {
      const input = "SELECT * FROMM glue.mydb.mytable"; // Note the typo in FROM
      await expect(transformer.transformGlueTableQuery(input)).rejects.toThrow();
    });

    it("should handle empty input", async () => {
      await expect(transformer.transformGlueTableQuery("")).rejects.toThrow(/no statements/);
    });

    it("should handle null input", async () => {
      // @ts-expect-error testing null input
      await expect(transformer.transformGlueTableQuery(null)).rejects.toThrow(
        /Cannot read properties/
      );
    });

    it("should handle malformed AST", async () => {
      const mockResult = {
        getRows: () => [[JSON.stringify({ invalid: "ast" })]],
      };
      jest.spyOn(db, "runAndReadAll").mockResolvedValueOnce(mockResult as any);

      await expect(transformer.transformGlueTableQuery("SELECT * FROM table")).rejects.toThrow(
        /no statements array/
      );
    });

    it("should handle invalid table references", async () => {
      const input = "SELECT * FROM glue..mytable"; // Missing database name
      await expect(transformer.transformGlueTableQuery(input)).rejects.toThrow();
    });
  });

  describe("SQL Comments and Formatting", () => {
    it("should handle complex table aliases", async () => {
      const input = `
      -- This is a comment
        SELECT t1.col1, t2.col2
        FROM glue.mydb.mytable AS t1
        JOIN glue.mydb.othertable AS t2
        ON t1.id = t2.id
      `;
      const output = await transformer.transformGlueTableQuery(input);
      expect(output).toContain("AS t1");
      expect(output).toContain("AS t2");
      expect(output).toContain("t1.col1");
      expect(output).toContain("t2.col2");
    });
  });

  describe("Edge Cases and Error Handling", () => {
    it("should handle deeply nested table references", async () => {
      const input = `
        WITH RECURSIVE cte AS (
          SELECT * FROM (
            SELECT * FROM (
              SELECT * FROM glue.mydb.mytable
            ) t1
          ) t2
        )
        SELECT * FROM cte
      `;
      const output = await transformer.transformGlueTableQuery(input);
      expect(output).toContain("parquet_scan(getvariable('mydb_mytable_files'))");
    });

    it("should handle multiple schema qualifiers", async () => {
      const input = "SELECT * FROM glue.mydb.schema.mytable";
      await expect(transformer.transformGlueTableQuery(input)).rejects.toThrow();
    });

    it("should handle case-sensitive identifiers", async () => {
      const input = `
        SELECT *
        FROM "GLUE"."MYDB"."MYTABLE"
        WHERE "COLUMN" = 'value'
      `;
      const output = await transformer.transformGlueTableQuery(input);
      // DuckDB normalizes identifiers to uppercase when quoted
      expect(output).toContain("parquet_scan(getvariable('MYDB_MYTABLE_files'))");
      expect(output).toContain('"COLUMN"');
    });

    it("should handle serialization errors", async () => {
      const mockResult = {
        getRows: () => [[null]],
      };
      jest.spyOn(db, "runAndReadAll").mockResolvedValueOnce(mockResult as any);

      await expect(transformer.transformGlueTableQuery("SELECT * FROM table")).rejects.toThrow(
        "Failed to serialize SQL query"
      );
    });

    it("should handle deserialization errors", async () => {
      // Mock successful serialization but failed deserialization
      const mockSerialize = {
        getRows: () => [[JSON.stringify({ statements: [] })]],
      };
      const mockDeserialize = {
        getRows: () => [[null]],
      };
      jest
        .spyOn(db, "runAndReadAll")
        .mockResolvedValueOnce(mockSerialize as any)
        .mockResolvedValueOnce(mockDeserialize as any);

      await expect(transformer.transformGlueTableQuery("SELECT * FROM table")).rejects.toThrow(
        "Failed to deserialize SQL query"
      );
    });
  });

  describe("Partition Filter Extraction", () => {
    it("should extract partition filters from WHERE clause", () => {
      const whereClause = {
        class: "COMPARISON",
        type: "COMPARE_EQUAL",
        left: {
          class: "COLUMN_REF",
          type: "COLUMN_REF",
          column_names: ["year"],
        },
        right: {
          class: "CONSTANT",
          type: "VALUE_CONSTANT",
          value: {
            type: { id: "VARCHAR" },
            is_null: false,
            value: "2024",
          },
        },
      };

      const filters = new Set<string>();
      (transformer as any).extractFiltersFromCondition(whereClause, ["year", "month"], filters);

      expect(Array.from(filters)).toEqual(["year = '2024'"]);
    });

    it("should handle complex partition filters with AND conditions", () => {
      const ast = {
        class: "CONJUNCTION",
        type: "CONJUNCTION_AND",
        children: [
          {
            class: "COMPARISON",
            type: "COMPARE_EQUAL",
            left: {
              class: "COLUMN_REF",
              type: "COLUMN_REF",
              column_names: ["year"],
            },
            right: {
              class: "CONSTANT",
              type: "VALUE_CONSTANT",
              value: {
                type: { id: "VARCHAR" },
                is_null: false,
                value: "2024",
              },
            },
          },
          {
            class: "OPERATOR",
            type: "COMPARE_IN",
            children: [
              {
                class: "COLUMN_REF",
                type: "COLUMN_REF",
                column_names: ["month"],
              },
              {
                class: "CONSTANT",
                type: "VALUE_CONSTANT",
                value: {
                  type: { id: "VARCHAR" },
                  is_null: false,
                  value: "01",
                },
              },
              {
                class: "CONSTANT",
                type: "VALUE_CONSTANT",
                value: {
                  type: { id: "VARCHAR" },
                  is_null: false,
                  value: "02",
                },
              },
              {
                class: "CONSTANT",
                type: "VALUE_CONSTANT",
                value: {
                  type: { id: "VARCHAR" },
                  is_null: false,
                  value: "03",
                },
              },
            ],
          },
        ],
      };

      const filters = new Set<string>();
      (transformer as any).extractFiltersFromCondition(ast, ["year", "month"], filters);

      const extractedFilters = Array.from(filters);
      expect(extractedFilters).toContain("year = '2024'");
      expect(extractedFilters).toContain("month IN ('01', '02', '03')");
    });

    it("should handle different expression types in conditions", () => {
      const conditions = [
        {
          class: "COMPARISON",
          type: "COMPARE_GREATERTHAN",
          left: {
            class: "COLUMN_REF",
            type: "COLUMN_REF",
            column_names: ["year"],
          },
          right: {
            class: "CONSTANT",
            type: "VALUE_CONSTANT",
            value: {
              type: { id: "INTEGER" },
              is_null: false,
              value: 2020,
            },
          },
        },
        {
          class: "COMPARISON",
          type: "COMPARE_LESSTHANOREQUALTO",
          left: {
            class: "COLUMN_REF",
            type: "COLUMN_REF",
            column_names: ["month"],
          },
          right: {
            class: "CONSTANT",
            type: "VALUE_CONSTANT",
            value: {
              type: { id: "VARCHAR" },
              is_null: false,
              value: "06",
            },
          },
        },
      ];

      const filters = new Set<string>();
      conditions.forEach((condition) => {
        (transformer as any).extractFiltersFromCondition(condition, ["year", "month"], filters);
      });

      const extractedFilters = Array.from(filters);
      expect(extractedFilters).toContain("year > 2020");
      expect(extractedFilters).toContain("month <= '06'");
    });

    it("should ignore non-partition columns in conditions", () => {
      const condition = {
        class: "CONJUNCTION",
        type: "CONJUNCTION_AND",
        children: [
          {
            class: "COMPARISON",
            type: "COMPARE_EQUAL",
            left: {
              class: "COLUMN_REF",
              type: "COLUMN_REF",
              column_names: ["year"],
            },
            right: {
              class: "CONSTANT",
              type: "VALUE_CONSTANT",
              value: {
                type: { id: "VARCHAR" },
                is_null: false,
                value: "2024",
              },
            },
          },
          {
            class: "COMPARISON",
            type: "COMPARE_GREATERTHAN",
            left: {
              class: "COLUMN_REF",
              type: "COLUMN_REF",
              column_names: ["amount"],
            },
            right: {
              class: "CONSTANT",
              type: "VALUE_CONSTANT",
              value: {
                type: { id: "INTEGER" },
                is_null: false,
                value: 1000,
              },
            },
          },
        ],
      };

      const filters = new Set<string>();
      (transformer as any).extractFiltersFromCondition(condition, ["year"], filters);

      const extractedFilters = Array.from(filters);
      expect(extractedFilters).toHaveLength(1);
      expect(extractedFilters).toContain("year = '2024'");
    });
  });

  describe("Error Handling and Edge Cases", () => {
    it("should handle empty AST statements array", async () => {
      const mockResult = {
        getRows: () => [[JSON.stringify({ statements: [] })]],
      };
      jest.spyOn(db, "runAndReadAll").mockResolvedValueOnce(mockResult as any);

      await expect(transformer.transformGlueTableQuery("SELECT * FROM table")).rejects.toThrow();
    });

    it("should handle missing node in AST statement", async () => {
      const mockResult = {
        getRows: () => [[JSON.stringify({ statements: [{}] })]],
      };
      jest.spyOn(db, "runAndReadAll").mockResolvedValueOnce(mockResult as any);

      await expect(transformer.transformGlueTableQuery("SELECT * FROM table")).rejects.toThrow();
    });

    it("should handle invalid JSON in AST", async () => {
      const mockResult = {
        getRows: () => [["{"]], // Invalid JSON
      };
      jest.spyOn(db, "runAndReadAll").mockResolvedValueOnce(mockResult as any);

      await expect(transformer.transformGlueTableQuery("SELECT * FROM table")).rejects.toThrow();
    });
  });

  describe("AST Fixture Tests", () => {
    let astFixtures: Record<string, { sql: string; ast: any }>;

    beforeAll(async () => {
      astFixtures = JSON.parse(
        (await fs.readFile(path.join(__dirname, "./fixtures/duckdb-ast.json"))).toString()
      );
    });

    it("should transform join query AST correctly", async () => {
      const { ast } = astFixtures.join_query;
      const transformer = new SqlTransformer(db);

      (transformer as any).transformNode(ast);

      // Verify both tables were transformed to parquet_scan
      const tableRefs = JSONPath({ json: ast, path: '$..*[?(@ && @.type=="TABLE_FUNCTION")]' });
      expect(tableRefs).toHaveLength(2);

      // Check first table transformation
      expect(tableRefs[0].function.function_name).toBe("parquet_scan");
      expect(tableRefs[0].function.children[0].children[0].value.value).toBe("db1_table1_files");

      // Check second table transformation
      expect(tableRefs[1].function.function_name).toBe("parquet_scan");
      expect(tableRefs[1].function.children[0].children[0].value.value).toBe("db2_table2_files");
    });

    it("should transform subquery AST correctly", async () => {
      const { ast } = astFixtures.subquery;
      const transformer = new SqlTransformer(db);

      (transformer as any).transformNode(ast);

      // Find the transformed table reference in the subquery
      const tableRefs = JSONPath({ json: ast, path: '$..*[?(@ && @.type=="TABLE_FUNCTION")]' });
      expect(tableRefs).toHaveLength(1);

      expect(tableRefs[0].function.function_name).toBe("parquet_scan");
      expect(tableRefs[0].function.children[0].children[0].value.value).toBe("mydb_mytable_files");
    });

    it("should transform complex CTE AST correctly", async () => {
      const { ast } = astFixtures.complex_cte;
      const transformer = new SqlTransformer(db);

      (transformer as any).transformNode(ast);

      // Find the transformed table reference in the CTE
      const tableRefs = JSONPath({
        json: ast,
        path: '$..*[?(@ && @.type=="TABLE_FUNCTION")]',
      });
      expect(tableRefs).toHaveLength(1);

      expect(tableRefs[0].function.function_name).toBe("parquet_scan");
      expect(tableRefs[0].function.children[0].children[0].value.value).toBe("mydb_mytable_files");

      // Verify the partition filters are preserved
      const whereClause = JSONPath({
        json: ast,
        path: '$..*[?(@ && @.type=="CONJUNCTION_AND")]',
      })[0];
      expect(whereClause.children).toHaveLength(2);
      expect(whereClause.children[0].type).toBe("COMPARE_EQUAL");
      expect(whereClause.children[1].type).toBe("COMPARE_IN");
    });

    it("should verify parquet_scan AST structure", async () => {
      const { ast } = astFixtures.parq;

      // Verify the structure matches what we generate
      const tableFunction = ast.statements[0].node.from_table;
      expect(tableFunction.type).toBe("TABLE_FUNCTION");
      expect(tableFunction.function.function_name).toBe("parquet_scan");
      expect(tableFunction.function.children[0].function_name).toBe("getvariable");
    });
  });

  describe("extractPartitionFilters", () => {
    it("should extract simple equality filter", async () => {
      const filters = await transformer.extractPartitionFilters(
        "SELECT * FROM tbl WHERE year = '2024'",
        "mydb.mytable",
        ["year"]
      );
      expect(filters).toHaveLength(1);
      expect(filters[0]).toBe("year = '2024'");
    });

    it("should handle multiple partition keys", async () => {
      const filters = await transformer.extractPartitionFilters(
        "SELECT * FROM tbl WHERE year = '2024' AND month = '01'",
        "mydb.mytable",
        ["year", "month"]
      );
      expect(filters).toContain("year = '2024'");
      expect(filters).toContain("month = '01'");
    });

    it("should ignore non-partition columns", async () => {
      const filters = await transformer.extractPartitionFilters(
        "SELECT * FROM tbl WHERE year = '2024' AND amount > 1000",
        "mydb.mytable",
        ["year"]
      );
      expect(filters).toHaveLength(1);
      expect(filters[0]).toBe("year = '2024'");
    });

    it("should handle IN conditions", async () => {
      const filters = await transformer.extractPartitionFilters(
        "SELECT * FROM tbl WHERE month IN ('01', '02')",
        "mydb.mytable",
        ["month"]
      );
      expect(filters).toHaveLength(1);
      expect(filters[0]).toBe("month IN ('01', '02')");
    });

    it("should handle complex conditions", async () => {
      const filters = await transformer.extractPartitionFilters(
        "SELECT * FROM tbl WHERE year = '2024' AND (month = '01' OR month = '02')",
        "mydb.mytable",
        ["year", "month"]
      );
      expect(filters).toContain("year = '2024'");
      expect(filters).toContain("month = '01'");
      expect(filters).toContain("month = '02'");
    });

    it("should handle invalid SQL gracefully", async () => {
      await expect(
        transformer.extractPartitionFilters("invalid sql", "mydb.mytable", ["year"])
      ).rejects.toThrow();
    });
  });

  describe("AST Manipulation", () => {
    it("should handle aliased table references", async () => {
      const input = "SELECT t.* FROM glue.mydb.mytable t";
      const output = await transformer.transformGlueTableQuery(input);
      expect(output).toBe("SELECT t.* FROM parquet_scan(getvariable('mydb_mytable_files')) AS t;");
    });

    it("should handle qualified column references", async () => {
      const input = "SELECT glue.mydb.mytable.col1 FROM glue.mydb.mytable";
      const output = await transformer.transformGlueTableQuery(input);
      expect(output).toContain("parquet_scan(getvariable('mydb_mytable_files'))");
    });
  });

  describe("Partition Filter Extraction Edge Cases", () => {
    it("should handle complex nested conditions", () => {
      const condition = {
        class: "CONJUNCTION",
        type: "CONJUNCTION_AND",
        children: [
          {
            class: "CONJUNCTION",
            type: "CONJUNCTION_OR",
            children: [
              {
                class: "COMPARISON",
                type: "COMPARE_EQUAL",
                left: {
                  class: "COLUMN_REF",
                  type: "COLUMN_REF",
                  column_names: ["year"],
                },
                right: {
                  class: "CONSTANT",
                  type: "VALUE_CONSTANT",
                  value: {
                    type: { id: "VARCHAR" },
                    is_null: false,
                    value: "2024",
                  },
                },
              },
              {
                class: "COMPARISON",
                type: "COMPARE_EQUAL",
                left: {
                  class: "COLUMN_REF",
                  type: "COLUMN_REF",
                  column_names: ["year"],
                },
                right: {
                  class: "CONSTANT",
                  type: "VALUE_CONSTANT",
                  value: {
                    type: { id: "VARCHAR" },
                    is_null: false,
                    value: "2023",
                  },
                },
              },
            ],
          },
          {
            class: "OPERATOR",
            type: "COMPARE_IN",
            children: [
              {
                class: "COLUMN_REF",
                type: "COLUMN_REF",
                column_names: ["month"],
              },
              {
                class: "CONSTANT",
                type: "VALUE_CONSTANT",
                value: {
                  type: { id: "VARCHAR" },
                  is_null: false,
                  value: "01",
                },
              },
              {
                class: "CONSTANT",
                type: "VALUE_CONSTANT",
                value: {
                  type: { id: "VARCHAR" },
                  is_null: false,
                  value: "02",
                },
              },
            ],
          },
        ],
      };

      const filters = new Set<string>();
      (transformer as any).extractFiltersFromCondition(condition, ["year", "month"], filters);

      const extractedFilters = Array.from(filters);
      expect(extractedFilters).toContain("month IN ('01', '02')");
      expect(extractedFilters).toContain("year = '2024'");
      expect(extractedFilters).toContain("year = '2023'");
    });

    it("should handle unsupported expression types", () => {
      const condition = {
        class: "COMPARISON",
        type: "COMPARE_EQUAL",
        left: {
          class: "COLUMN_REF",
          type: "COLUMN_REF",
          column_names: ["year"],
        },
        right: {
          class: "UNSUPPORTED",
          type: "UNSUPPORTED_TYPE",
          value: "something",
        },
      };

      const filters = new Set<string>();
      (transformer as any).extractFiltersFromCondition(condition, ["year"], filters);

      expect(Array.from(filters)).toHaveLength(0);
    });

    it("should handle null conditions gracefully", () => {
      const filters = new Set<string>();
      (transformer as any).extractFiltersFromCondition(null, ["year"], filters);
      expect(Array.from(filters)).toHaveLength(0);
    });

    it("should handle invalid operator conditions", () => {
      const condition = {
        class: "OPERATOR",
        type: "INVALID_OPERATOR",
        children: [],
      };

      const filters = new Set<string>();
      (transformer as any).extractFiltersFromCondition(condition, ["year"], filters);
      expect(Array.from(filters)).toHaveLength(0);
    });
  });

  describe("View Creation SQL", () => {
    it("should generate view creation SQL", async () => {
      const viewSql = await transformer.getGlueTableViewSql(
        "SELECT col1, col2 FROM glue.mydb.mytable WHERE id > 100"
      );
      expect(viewSql[0]).toBe(
        "CREATE OR REPLACE VIEW mydb_mytable_gview AS SELECT * FROM parquet_scan(getvariable('mydb_mytable_gview_files'));"
      );
    });

    it("should handle complex queries in view creation", async () => {
      const viewSql = await transformer.getGlueTableViewSql(
        `SELECT t1.col1, t2.col2 
         FROM glue.db1.table1 t1 
         JOIN glue.db2.table2 t2 ON t1.id = t2.id`
      );
      expect(viewSql[0]).toContain("CREATE OR REPLACE VIEW db1_table1_gview AS");
      expect(viewSql[0]).toContain("parquet_scan(getvariable('db1_table1_gview_files'))");
      expect(viewSql[1]).toContain("CREATE OR REPLACE VIEW db2_table2_gview AS");
      expect(viewSql[1]).toContain("parquet_scan(getvariable('db2_table2_gview_files'))");
    });
  });
});
