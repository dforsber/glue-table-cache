import { DuckDBConnection } from "@duckdb/node-api";
import { TableReference } from "./types.js";
export declare class SqlTransformer {
    private db;
    constructor(db: DuckDBConnection);
    private getQueryAST;
    private getSqlFromAst;
    transformGlueTableQuery(query: string): Promise<string>;
    getQueryGlueTableRefs(query: string): Promise<TableReference[]>;
    private getAstGlueTableRefs;
    private getAstTableRefs;
    private transformNode;
    private getGlueTableRef;
    extractPartitionFilters(query: string, tableName: string, partitionKeys: string[]): Promise<string[]>;
    private findPartitionFilters;
    private extractFiltersFromCondition;
    private getComparisonOperator;
    getQueryFilesVarName(database: string, table: string): string;
    getGlueTableFilesVarName(database: string, table: string): string;
    getGlueTableViewSql(query: string): Promise<string[]>;
}
