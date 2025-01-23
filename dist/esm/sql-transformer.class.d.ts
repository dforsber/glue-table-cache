import { DuckDBConnection } from "@duckdb/node-api";
import { TableReference } from "./types.js";
export declare class SqlTransformer {
    private db;
    private prefix;
    constructor(db: DuckDBConnection, prefix: string);
    private getQueryAST;
    private getSqlFromAst;
    transformTableQuery(query: string): Promise<string>;
    getQueryTableRefs(query: string): Promise<TableReference[]>;
    private getAstTableRefs;
    private getAstTableRefsWithNode;
    private transformNode;
    private getTableRef;
    extractPartitionFilters(query: string, partitionKeys: string[]): Promise<string[]>;
    private findPartitionFilters;
    private extractFiltersFromCondition;
    private getComparisonOperator;
    getQueryFilesVarName(database: string, table: string): string;
    getTableFilesVarName(database: string, table: string): string;
    getTableViewName(database: string, table: string): string;
    getTableViewSql(query: string, s3filesLength?: number): Promise<string[]>;
}
