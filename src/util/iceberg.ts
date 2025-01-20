import { v4 as uuidv4 } from "uuid";

/**
 *
 * @param baseS3url base S3 URL
 * @param prefix used for setting the SQL VARIABLES and avoiding clashes with other possibly concurrent statements
 * @param _offset must be >=0. Latest/current table version is 0. Previous veresion is offset==1 etc.
 *
 * @returns SQL statements array that need to be all executed. The last statement returns the paths.
 */
export function getIcebergS3FilesStmts(baseS3url: string, prefix?: string, _offset = 0): string[] {
  const stmts: string[] = [];
  const offset = _offset < 0 ? 0 : _offset;
  const id = prefix ?? "a" + uuidv4().replaceAll("-", "");
  const s3path = baseS3url.endsWith("/") ? baseS3url : baseS3url + "/";
  stmts.push(
    "INSTALL avro FROM community",
    "LOAD avro",
    // 0. Figure out all Iceberg Table versions (through all manifest files)
    `SET VARIABLE ${id}_metadata_json_path = ( ` +
      `SELECT filename ` +
      `FROM read_json_auto('${s3path}metadata/*.json', filename=1) ` +
      `ORDER BY "last-sequence-number" DESC OFFSET ${offset} LIMIT 1 )`,
    // 1. Pick the latest Iceberg Table version from the manifest file
    `SET VARIABLE ${id}_manifest_list_avro_file = ( ` +
      `SELECT snapshots[len(snapshots)]['manifest-list'] AS path ` +
      `FROM read_json(getvariable('${id}_metadata_json_path')))`,
    // 2. snapshot manifest-list Avro file
    `SET VARIABLE ${id}_manifests = ( ` +
      `SELECT list(manifest_path) AS path ` +
      `FROM read_avro(getvariable('${id}_manifest_list_avro_file')))`,
    // 3. Get file paths based on the actual manifest Avro file
    `SELECT data_file['file_path'] AS path FROM read_avro(getvariable('${id}_manifests'))`
  );
  return stmts;
}
