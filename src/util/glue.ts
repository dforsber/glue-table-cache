import {
  GetPartitionsCommand,
  GetTableCommand,
  GetTableRequest,
  GlueClient,
  Table,
} from "@aws-sdk/client-glue";
import { CachedTableMetadata, ETableType, ProjectionPattern } from "../types.js";
import { debug } from "./debug.js";

const log = debug("glue-api");

function getTableType(tbl: Table): ETableType {
  const p = tbl.Parameters ?? {};
  if (p.table_type === "ICEBERG") return ETableType.ICEBERG;
  if (p["projection.enabled"] === "true") return ETableType.GLUE_PROJECTED;
  if (tbl.PartitionKeys && tbl.PartitionKeys.length) return ETableType.HIVE;
  return ETableType.UNPARTITIONED;
}

export async function getGlueTableMetadata(
  gluecli: GlueClient,
  DatabaseName: string,
  Name: string
): Promise<CachedTableMetadata> {
  try {
    const tableRequest: GetTableRequest = { DatabaseName, Name };
    const tableResponse = await gluecli.send(new GetTableCommand(tableRequest));
    const table = tableResponse.Table;
    if (!table) throw new Error(`Table ${DatabaseName}.${Name} not found`);
    // Glue Table Parameters include for example:
    //  table_type: ICEBERG
    //  metadata_location: 's3://athena-results-dforsber/iceberg_table/metadata/00000-f607b49b-1780-421e-bf2b-6b00cc16230e.metadata.json'
    const tableType = getTableType(table);
    const metadata: CachedTableMetadata = { timestamp: Date.now(), table: table, tableType };

    // Handle partition projection if enabled
    if (tableType === ETableType.GLUE_PROJECTED && table.Parameters) {
      metadata.projectionPatterns = parseProjectionPatterns(table.Parameters);
    } else if (tableType === ETableType.HIVE) {
      // Load partition metadata for standard partitioned tables
      metadata.partitionMetadata = await loadPartitionMetadata(gluecli, DatabaseName, Name);
    }
    return metadata;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
  } catch (err: any) {
    log("getGlueTableMetadata ERROR:", err);
    throw err;
  }
}

function parseProjectionValue(property: string, value: string): string | string[] | number[] {
  switch (property) {
    case "type":
      return value as "enum" | "integer" | "date" | "injected";
    case "format":
      return value;
    case "range":
      // Handle both JSON array format and comma-separated format
      try {
        return JSON.parse(value);
      } catch {
        return value.split(",").map((v) => v.trim());
      }
    case "values":
      return JSON.parse(value);
    default:
      return value;
  }
}

export function parseProjectionPatterns(parameters: Record<string, string>): {
  enabled: boolean;
  patterns: Record<string, ProjectionPattern>;
} {
  const patterns: Record<string, ProjectionPattern> = {};

  Object.entries(parameters)
    .filter(([key]) => key.startsWith("projection."))
    .forEach(([key, value]) => {
      const match = key.match(/projection\.(\w+)\.(type|range|format|values)/);
      if (match) {
        const [_, column, property] = match;
        if (!patterns[column]) {
          patterns[column] = { type: "enum" };
        }
        if (
          property === "type" ||
          property === "format" ||
          property === "range" ||
          property === "values"
        ) {
          (patterns[column] as ProjectionPattern as unknown as Record<string, unknown>)[property] =
            parseProjectionValue(property, value);
        }
      }
    });

  return {
    enabled: true,
    patterns,
  };
}

export async function loadPartitionMetadata(
  gluecli: GlueClient,
  DatabaseName: string,
  TableName: string
) {
  // Implementation for loading standard partition metadata
  const command = new GetPartitionsCommand({
    DatabaseName,
    TableName,
    // Add pagination handling for large partition sets
  });

  try {
    const response = await gluecli.send(command);
    if (!response.Partitions || response.Partitions.length === 0) {
      return { keys: [], values: [] };
    }
    return {
      keys: response.Partitions[0].Values || [],
      values:
        response.Partitions.map((p) => ({
          values: p.Values || [],
          location: p.StorageDescriptor?.Location,
        })) || [],
    };
  } catch (error) {
    console.warn(`Failed to load partitions for ${DatabaseName}_${TableName}:`, error);
    return { keys: [], values: [] };
  }
}

export async function getPartitionExtractor(
  key: string,
  metadata: CachedTableMetadata
): Promise<string> {
  // Check if this is a projection-enabled table
  if (metadata.projectionPatterns?.enabled) {
    const pattern = metadata.projectionPatterns.patterns[key];
    if (!pattern) {
      throw new Error(`No projection pattern found for partition key ${key}`);
    }

    // Handle different projection types
    switch (pattern.type) {
      case "date":
        // For date projections, use the format pattern to build regex
        const dateFormat = pattern.format || "yyyy-MM-dd";
        const dateRegex = convertDateFormatToRegex(dateFormat);
        return `regexp_extract(path, '(${dateRegex})', 1)`;

      case "integer":
        // For integer projections, extract full numeric values
        return "CAST(regexp_extract(path, '/([0-9]+)/', 1) AS INTEGER)";

      case "enum":
        // For enum projections, extract the last path component before the filename
        return "regexp_extract(path, '/([^/]+)/[^/]*$', 1)";

      case "injected":
        // For injected values, extract them from the SQL query filters
        // The query must contain static equality conditions
        throw new Error("Injected partition values not supported yet");

      default:
        throw new Error(`Unsupported projection type: ${pattern.type}`);
    }
  }

  // Default to Hive-style partitioning
  return `regexp_extract(path, '${key}=([^/]+)', 1)`;
}

export function convertDateFormatToRegex(format: string): string {
  // Convert Java SimpleDateFormat patterns to regex patterns
  const conversions: Record<string, string> = {
    yyyy: "\\d{4}",
    MM: "\\d{2}",
    dd: "\\d{2}",
    HH: "\\d{2}",
    mm: "\\d{2}",
    ss: "\\d{2}",
  };

  let regex = format;
  for (const [pattern, replacement] of Object.entries(conversions)) {
    regex = regex.replace(pattern, replacement);
  }
  return regex;
}
