"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.ETableType = void 0;
var ETableType;
(function (ETableType) {
    ETableType["HIVE"] = "HIVE";
    ETableType["ICEBERG"] = "ICEBERG";
    ETableType["HUDI"] = "HUDI";
    ETableType["DELTA"] = "DELTA";
    ETableType["GLUE_PROJECTED"] = "GLUE_PROJECTED";
    ETableType["UNPARTITIONED"] = "UNPARTITIONED";
})(ETableType || (exports.ETableType = ETableType = {}));
