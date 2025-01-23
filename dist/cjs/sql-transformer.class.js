"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.SqlTransformer = void 0;
const jsonpath_plus_1 = require("jsonpath-plus");
const debug_js_1 = require("./util/debug.js");
const log = (0, debug_js_1.debug)("sql-table-cache:sql");
const logAst = (0, debug_js_1.debug)("sql-table-cache:sql:ast");
class SqlTransformer {
    db;
    prefix;
    constructor(db, prefix) {
        this.db = db;
        this.prefix = prefix;
    }
    async getQueryAST(query) {
        // Get the AST in JSON format
        const sqlCmd = `SELECT json_serialize_sql('${query.replace(/'/g, "''")}')`;
        log("Serializing SQL: %s", sqlCmd);
        const result = await this.db.runAndReadAll(sqlCmd);
        const rows = result.getRows();
        if (!rows.length || rows[0].length === 0 || rows[0][0] === null) {
            throw new Error("Failed to serialize SQL query");
        }
        const ast = JSON.parse(rows[0][0]);
        if (ast.error)
            throw new Error(JSON.stringify(ast));
        return ast;
    }
    async getSqlFromAst(ast) {
        const deserializeCmd = `SELECT json_deserialize_sql('${JSON.stringify(ast).replace(/'/g, "''")}')`;
        log("Deserializing SQL: %s", deserializeCmd);
        const transformed = await this.db.runAndReadAll(deserializeCmd);
        const rows = transformed.getRows();
        if (!rows.length || rows[0].length === 0 || rows[0][0] === null) {
            throw new Error("Failed to deserialize SQL query");
        }
        return rows[0][0] + ";";
    }
    async transformTableQuery(query) {
        log("Transforming query: %s", query);
        // Get the AST in JSON format
        const ast = await this.getQueryAST(query);
        logAst("Original AST: %O", ast);
        // Transform the AST
        this.transformNode(ast);
        logAst("Transformed AST: %O", ast);
        // Convert back to SQL
        const sql = await this.getSqlFromAst(ast);
        log("Transformed query: %s", sql);
        return sql;
    }
    async getQueryTableRefs(query) {
        // Get the AST in JSON format
        const ast = await this.getQueryAST(query);
        logAst("Original AST: %O", ast);
        return this.getAstTableRefs(ast);
    }
    getAstTableRefs(ast) {
        return this.getAstTableRefsWithNode(ast)
            .map((ref) => ref.tableRef)
            .filter(Boolean);
    }
    getAstTableRefsWithNode(ast) {
        const pathExpr = `$..*[?(@ && @.type=='BASE_TABLE' && ` +
            `(@.catalog_name=='${this.prefix.toLowerCase()}' || ` +
            `@.catalog_name=='${this.prefix.toUpperCase()}'))]`;
        const tableRefPaths = (0, jsonpath_plus_1.JSONPath)({ path: pathExpr, json: ast });
        const tableRefsWithNode = tableRefPaths.map((node) => ({
            node,
            tableRef: this.getTableRef(node),
        }));
        return tableRefsWithNode;
    }
    transformNode(ast) {
        log(`Finding ${this.prefix} table references in AST`);
        logAst("AST structure:", ast);
        // Find all table references using JSONPath
        const tableRefs = this.getAstTableRefsWithNode(ast);
        log(`Found ${tableRefs.length} ${this.prefix} table references`);
        logAst("Table references:", tableRefs);
        // Remove all query_location keys
        const paths = (0, jsonpath_plus_1.JSONPath)({
            path: "$..query_location",
            json: ast,
            resultType: "pointer", // Get JSON pointers instead
        });
        paths.forEach((pointer) => {
            const segments = pointer.split("/").filter(Boolean);
            segments.pop(); // Remove 'query_location'
            let parent = ast;
            // Navigate to parent
            for (const segment of segments) {
                parent = parent[segment];
            }
            // Set query_location to undefined
            if (parent) {
                parent.query_location = undefined;
            }
        });
        // Transform each table reference
        for (const ref of tableRefs) {
            const tableRef = ref.tableRef;
            if (tableRef) {
                log("Transforming table reference %s.%s", tableRef.database, tableRef.table);
                // Replace with parquet_scan function call
                Object.assign(ref.node, {
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
                                            value: this.getQueryFilesVarName(tableRef.database, tableRef.table),
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
    getTableRef(node) {
        if (node.type === "BASE_TABLE" &&
            (node.catalog_name === this.prefix.toLowerCase() ||
                node.catalog_name === this.prefix.toUpperCase())) {
            return {
                database: node.schema_name || "default",
                table: node.table_name,
            };
        }
        return null;
    }
    async extractPartitionFilters(query, partitionKeys) {
        // Get the AST in JSON format
        const sqlCmd = `SELECT json_serialize_sql('${query.replace(/'/g, "''")}')`;
        log("Serializing SQL: %s", sqlCmd);
        const result = await this.db.runAndReadAll(sqlCmd);
        const rows = result.getRows();
        if (!rows.length || rows[0].length === 0 || rows[0][0] === null) {
            throw new Error("Failed to serialize SQL query");
        }
        const ast = JSON.parse(rows[0][0]);
        if (ast.error)
            throw new Error(JSON.stringify(ast));
        logAst("Original AST: %O", ast);
        const filters = new Set();
        if (ast?.statements?.[0]?.node?.where_clause) {
            this.extractFiltersFromCondition(ast.statements[0].node.where_clause, partitionKeys, filters);
        }
        return Array.from(filters);
    }
    findPartitionFilters(node, tableName, partitionKeys, filters) {
        if (!node || typeof node !== "object")
            return;
        // Check if this is a WHERE clause
        if (node.type === "SELECT" && node.where) {
            this.extractFiltersFromCondition(node.where, partitionKeys, filters);
        }
        // Recursively process all properties
        for (const key in node) {
            if (Array.isArray(node[key])) {
                node[key].forEach((child) => this.findPartitionFilters(child, tableName, partitionKeys, filters));
            }
            else if (typeof node[key] === "object") {
                this.findPartitionFilters(node[key], tableName, partitionKeys, filters);
            }
        }
    }
    extractFiltersFromCondition(condition, partitionKeys, filters) {
        if (!condition || typeof condition !== "object")
            return;
        // Handle COMPARISON nodes
        if (condition.class === "COMPARISON") {
            const left = condition.left;
            const right = condition.right;
            if (left?.class === "COLUMN_REF" &&
                partitionKeys.includes(left.column_names?.[0]) &&
                right?.class === "CONSTANT" &&
                right?.type === "VALUE_CONSTANT") {
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
            condition.children?.forEach((child) => this.extractFiltersFromCondition(child, partitionKeys, filters));
        }
        // Handle IN conditions
        else if (condition.class === "OPERATOR" && condition.type === "COMPARE_IN") {
            const left = condition.children?.[0];
            if (left?.class === "COLUMN_REF" && partitionKeys.includes(left.column_names?.[0])) {
                const values = condition.children
                    .slice(1)
                    .filter((c) => c.class === "CONSTANT" && c.type === "VALUE_CONSTANT")
                    .map((c) => `'${c.value.value}'`);
                if (values.length > 0) {
                    filters.add(`${left.column_names[0]} IN (${values.join(", ")})`);
                }
            }
        }
    }
    getComparisonOperator(type) {
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
    getQueryFilesVarName(database, table) {
        return `${database}_${table}_files`.replaceAll("-", "");
    }
    getTableFilesVarName(database, table) {
        return `${database}_${table}_${this.prefix}_files`.replaceAll("-", "");
    }
    getTableViewName(database, table) {
        return `${this.prefix}__${database}_${table}`.replaceAll("-", "");
    }
    async getTableViewSql(query, s3filesLength = 1) {
        // Get the AST in JSON format to extract table references
        const sqlCmd = `SELECT json_serialize_sql('${query.replace(/'/g, "''")}')`;
        const result = await this.db.runAndReadAll(sqlCmd);
        const rows = result.getRows();
        if (!rows.length || rows[0].length === 0 || rows[0][0] === null) {
            throw new Error("Failed to serialize SQL query");
        }
        const ast = JSON.parse(rows[0][0]);
        if (ast.error)
            throw new Error(JSON.stringify(ast));
        // Find all table references
        const tableRefs = this.getAstTableRefs(ast);
        if (!tableRefs.length)
            throw new Error("No table references found in query");
        // Create a view for each unique table reference
        const views = [];
        const processedTables = new Set();
        for (const ref of tableRefs) {
            const tablVarName = this.getTableFilesVarName(ref.database, ref.table);
            const tableKey = `${ref.database}_${ref.table}`;
            if (!processedTables.has(tableKey)) {
                processedTables.add(tableKey);
                const tableViewName = this.getTableViewName(ref.database, ref.table);
                const baseQuery = s3filesLength
                    ? `SELECT * FROM parquet_scan(getvariable('${tablVarName}'))`
                    : `SELECT NULL LIMIT 0`;
                views.push(`CREATE OR REPLACE VIEW ${tableViewName} AS ${baseQuery};`);
            }
        }
        return views;
    }
}
exports.SqlTransformer = SqlTransformer;
