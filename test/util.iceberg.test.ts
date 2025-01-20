import { getIcebergS3FilesStmts } from "../src/util/iceberg";

describe("iceberg", () => {
  it("sql", () => {
    expect(getIcebergS3FilesStmts("s3://BUCKET/PREFIX").length).toEqual(6);
    expect(getIcebergS3FilesStmts("s3://BUCKET/PREFIX", "b123")).toEqual([
      `INSTALL avro FROM community`,
      `LOAD avro`,
      `SET VARIABLE b123_metadata_json_path = ( SELECT filename FROM read_json_auto('s3://BUCKET/PREFIX/metadata/*.json', filename=1) ORDER BY "last-sequence-number" DESC OFFSET 0 LIMIT 1 )`,
      `SET VARIABLE b123_manifest_list_avro_file = ( SELECT snapshots[len(snapshots)]['manifest-list'] AS path FROM read_json(getvariable('b123_metadata_json_path')))`,
      `SET VARIABLE b123_manifests = ( SELECT list(manifest_path) AS path FROM read_avro(getvariable('b123_manifest_list_avro_file')))`,
      `SELECT data_file['file_path'] AS path FROM read_avro(getvariable('b123_manifests'))`,
    ]);
  });
});
