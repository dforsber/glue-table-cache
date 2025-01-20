import { Table } from "@aws-sdk/client-glue";

export enum ETableType {
  HIVE = "HIVE",
  ICEBERG = "ICEBERG",
  HUDI = "HUDI",
  DELTA = "DELTA",
  GLUE_PROJECTED = "GLUE_PROJECTED",
  UNPARTITIONED = "UNPARTITIONED",
}

export interface S3FileInfo {
  path: string;
  partitionValues: Record<string, string>;
}

export interface CachedTableMetadata {
  timestamp: number;
  tableType: ETableType;
  table: Table;
  partitionMetadata?: {
    keys: string[];
    values: Array<{
      values?: string[];
      location?: string;
    }>;
  };
  projectionPatterns?: {
    enabled: boolean;
    patterns: Record<string, ProjectionPattern>;
  };
}

export interface ProjectionPattern {
  type: "enum" | "integer" | "date" | "injected";
  format?: string;
  range?: [string | number, string | number];
  values?: (string | number)[];
}

export interface CacheConfig {
  region: string;
  maxEntries: number;
  forceRefreshOnError: boolean;
  glueTableMetadataTtlMs: number;
  s3ListingRefresTtlhMs: number;
  proxyAddress?: string;
}

export interface CacheEntry<T> {
  timestamp: number;
  data: T;
}

export interface TableReference {
  database: string;
  table: string;
}
