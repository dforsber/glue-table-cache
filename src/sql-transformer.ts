import { DuckDBConnection } from "@duckdb/node-api";
import debug from "debug";
import jp from "jsonpath";

const log = debug("glue-table-cache:sql");
const logAst = debug("glue-table-cache:sql:ast");

interface TableReference {
  database: string;
  table: string;
}

export class SqlTransformer {
  constructor(private db: DuckDBConnection) {}

  async transformGlueTableQuery(query: string): Promise<string> {
    log("Transforming query: %s", query);

    // Get the AST in JSON format
    const sqlCmd = `SELECT json_serialize_sql('${query.replace(/'/g, "''")}')`;
    log("Serializing SQL: %s", sqlCmd);
    const result = await this.db.runAndReadAll(sqlCmd);
    let rows = result.getRows();
    if (!rows.length || rows[0].length === 0 || rows[0][0] === null) {
      throw new Error("Failed to serialize SQL query");
    }
    const ast = JSON.parse(rows[0][0] as string);
    if (ast.error) throw new Error(JSON.stringify(ast));
    logAst("Original AST: %O", ast);

    // Transform the AST
    this.transformNode(ast);
    logAst("Transformed AST: %O", ast);

    // Convert back to SQL
    // Convert back to SQL
    const deserializeCmd = `SELECT json_deserialize_sql('${JSON.stringify(ast).replace(/'/g, "''")}')`;
    log("Deserializing SQL: %s", deserializeCmd);
    const transformed = await this.db.runAndReadAll(deserializeCmd);
    rows = transformed.getRows();
    if (!rows.length || rows[0].length === 0 || rows[0][0] === null) {
      throw new Error("Failed to deserialize SQL query");
    }
    const sql = (rows[0][0] as string) + ";";
    log("Transformed query: %s", sql);
    return sql;
  }

  private removeQueryLocation(ast: any): void {
    // Remove all query_location keys
    jp.apply(ast, "$..query_location", () => undefined);
  }

  private transformNode(ast: any): void {
    log("Finding Glue table references in AST");
    logAst("AST structure:", ast);

    // Find all Glue table references using JSONPath
    const tableRefs = jp.query(
      ast,
      '$..*[?(@.type=="BASE_TABLE" && ( @.catalog_name=="glue" || @.catalog_name=="GLUE" ))]'
    );
    log("Found %d Glue table references", tableRefs.length);
    logAst("Table references:", tableRefs);

    // remove all query_location keys
    this.removeQueryLocation(ast);

    // Transform each table reference
    for (const match of tableRefs) {
      const node = match;
      const tableRef = this.extractTableReference(node);
      if (tableRef) {
        log("Transforming table reference %s.%s", tableRef.database, tableRef.table);
        // Replace with parquet_scan function call
        Object.assign(node, {
          type: "TABLE_FUNCTION",
          function: {
            class: "FUNCTION",
            type: "FUNCTION",
            function_name: "parquet_scan",
            schema: "",
            catalog: "",
            children: [
              {
                class: "FUNCTION",
                type: "FUNCTION",
                function_name: "getvariable",
                schema: "",
                catalog: "",
                children: [
                  {
                    class: "CONSTANT",
                    type: "VALUE_CONSTANT",
                    value: {
                      type: {
                        id: "VARCHAR",
                        type_info: null,
                      },
                      is_null: false,
                      value: `${tableRef.database}_${tableRef.table}_files`,
                    },
                  },
                ],
              },
            ],
            filter: null,
            order_bys: { type: "ORDER_MODIFIER", orders: [] },
            distinct: false,
            is_operator: false,
            export_state: false,
          },
        });
      }
    }
  }

  private extractTableReference(node: any): TableReference | null {
    if (
      node.type === "BASE_TABLE" &&
      (node.catalog_name === "glue" || node.catalog_name === "GLUE")
    ) {
      return {
        database: node.schema_name || "default",
        table: node.table_name,
      };
    }
    return null;
  }

  async extractPartitionFilters(
    query: string,
    tableName: string,
    partitionKeys: string[]
  ): Promise<string[]> {
    // Get the AST in JSON format
    const sqlCmd = `SELECT json_serialize_sql('${query.replace(/'/g, "''")}')`;
    log("Serializing SQL: %s", sqlCmd);
    const result = await this.db.runAndReadAll(sqlCmd);
    const rows = result.getRows();
    if (!rows.length || rows[0].length === 0 || rows[0][0] === null) {
      throw new Error("Failed to serialize SQL query");
    }
    const ast = JSON.parse(rows[0][0] as string);
    if (ast.error) throw new Error(JSON.stringify(ast));
    logAst("Original AST: %O", ast);

    const filters: Set<string> = new Set();
    if (ast?.statements?.[0]?.node?.where_clause) {
      this.extractFiltersFromCondition(ast.statements[0].node.where_clause, partitionKeys, filters);
    }
    return Array.from(filters);
  }

  private findPartitionFilters(
    node: any,
    tableName: string,
    partitionKeys: string[],
    filters: Set<string>
  ): void {
    if (!node || typeof node !== "object") return;

    // Check if this is a WHERE clause
    if (node.type === "SELECT" && node.where) {
      this.extractFiltersFromCondition(node.where, partitionKeys, filters);
    }

    // Recursively process all properties
    for (const key in node) {
      if (Array.isArray(node[key])) {
        node[key].forEach((child: any) =>
          this.findPartitionFilters(child, tableName, partitionKeys, filters)
        );
      } else if (typeof node[key] === "object") {
        this.findPartitionFilters(node[key], tableName, partitionKeys, filters);
      }
    }
  }

  private extractFiltersFromCondition(
    condition: any,
    partitionKeys: string[],
    filters: Set<string>
  ): void {
    if (!condition || typeof condition !== "object") return;

    // Handle COMPARISON nodes
    if (condition.class === "COMPARISON") {
      const left = condition.left;
      const right = condition.right;
      if (
        left?.class === "COLUMN_REF" &&
        partitionKeys.includes(left.column_names?.[0]) &&
        right?.class === "CONSTANT" &&
        right?.type === "VALUE_CONSTANT"
      ) {
        const value = right.value?.value;
        if (value !== undefined) {
          const operator = this.getComparisonOperator(condition.type);
          const quotedValue = typeof value === "string" ? `'${value}'` : value;
          filters.add(`${left.column_names[0]} ${operator} ${quotedValue}`);
        }
      }
    }
    // Handle CONJUNCTION nodes (AND/OR)
    else if (condition.class === "CONJUNCTION") {
      condition.children?.forEach((child: any) =>
        this.extractFiltersFromCondition(child, partitionKeys, filters)
      );
    }
    // Handle IN conditions
    else if (condition.class === "OPERATOR" && condition.type === "COMPARE_IN") {
      const left = condition.children?.[0];
      if (left?.class === "COLUMN_REF" && partitionKeys.includes(left.column_names?.[0])) {
        const values = condition.children
          .slice(1)
          .filter((c: any) => c.class === "CONSTANT" && c.type === "VALUE_CONSTANT")
          .map((c: any) => `'${c.value.value}'`);
        if (values.length > 0) {
          filters.add(`${left.column_names[0]} IN (${values.join(", ")})`);
        }
      }
    }
  }

  private getComparisonOperator(type: string): string {
    switch (type) {
      case "COMPARE_EQUAL":
        return "=";
      case "COMPARE_GREATERTHAN":
        return ">";
      case "COMPARE_LESSTHAN":
        return "<";
      case "COMPARE_GREATERTHANOREQUALTO":
        return ">=";
      case "COMPARE_LESSTHANOREQUALTO":
        return "<=";
      case "COMPARE_NOTEQUAL":
        return "!=";
      default:
        throw new Error(`Unsupported comparison type: ${type}`);
    }
  }

  async getGlueTableViewSql(query: string): Promise<string[]> {
    // Get the AST in JSON format to extract table references
    const sqlCmd = `SELECT json_serialize_sql('${query.replace(/'/g, "''")}')`;
    const result = await this.db.runAndReadAll(sqlCmd);
    const rows = result.getRows();
    if (!rows.length || rows[0].length === 0 || rows[0][0] === null) {
      throw new Error("Failed to serialize SQL query");
    }
    const ast = JSON.parse(rows[0][0] as string);
    if (ast.error) throw new Error(JSON.stringify(ast));

    // Find all Glue table references
    const tableRefs = jp.query(
      ast,
      '$..*[?(@.type=="BASE_TABLE" && (@.catalog_name=="glue" || @.catalog_name=="GLUE"))]'
    );

    if (!tableRefs.length) {
      throw new Error("No Glue table references found in query");
    }

    // Create a view for each unique table reference
    const views: string[] = [];
    const processedTables = new Set<string>();

    for (const ref of tableRefs) {
      const tableKey = `${ref.schema_name}_${ref.table_name}`;
      if (!processedTables.has(tableKey)) {
        processedTables.add(tableKey);
        const tableViewName = `${tableKey}_gview`;
        const baseQuery = `SELECT * FROM parquet_scan(getvariable('${tableKey}_files'))`;
        views.push(`CREATE OR REPLACE VIEW ${tableViewName} AS ${baseQuery};`);
      }
    }

    return views;
  }
}
