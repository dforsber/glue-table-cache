/**
 *
 * @param baseS3url base S3 URL
 * @param prefix used for setting the SQL VARIABLES and avoiding clashes with other possibly concurrent statements
 * @param _offset must be >=0. Latest/current table version is 0. Previous veresion is offset==1 etc.
 *
 * @returns SQL statements array that need to be all executed. The last statement returns the paths.
 */
export declare function getIcebergS3FilesStmts(baseS3url: string, prefix?: string, _offset?: number): string[];
