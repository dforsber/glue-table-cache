export var ETableType;
(function (ETableType) {
    ETableType["HIVE"] = "HIVE";
    ETableType["ICEBERG"] = "ICEBERG";
    ETableType["HUDI"] = "HUDI";
    ETableType["DELTA"] = "DELTA";
    ETableType["GLUE_PROJECTED"] = "GLUE_PROJECTED";
    ETableType["UNPARTITIONED"] = "UNPARTITIONED";
    ETableType["S3_TABLE"] = "S3_TABLE";
})(ETableType || (ETableType = {}));
