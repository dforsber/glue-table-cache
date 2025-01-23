/* eslint-disable @typescript-eslint/no-explicit-any */
import { GetPartitionsCommand, GetTableCommand, GlueClient } from "@aws-sdk/client-glue";
import {
  convertDateFormatToRegex,
  getGlueTableMetadata,
  getPartitionExtractor,
  parseProjectionPatterns,
} from "../src/util/glue";
import { mockClient } from "aws-sdk-client-mock";
import { CachedGlueTableMetadata, ETableType } from "../src/types";

const glueMock = mockClient(GlueClient);
let glueCli: GlueClient;

describe("glue", () => {
  beforeEach(() => {
    glueMock.reset();
    glueCli = new GlueClient();
  });

  it("should handle date format conversion correctly", async () => {
    // Test various date format patterns
    const patterns = {
      yyyy: "\\d{4}",
      MM: "\\d{2}",
      dd: "\\d{2}",
      "yyyy-MM-dd": "\\d{4}-\\d{2}-\\d{2}",
      "yyyy/MM/dd": "\\d{4}/\\d{2}/\\d{2}",
      yyyyMMdd: "\\d{4}\\d{2}\\d{2}",
    };

    for (const [format, expected] of Object.entries(patterns)) {
      const result = convertDateFormatToRegex(format);
      expect(result).toBe(expected);
    }
  });

  it("should handle partition projection tables", async () => {
    // Mock Glue response for projection-enabled table
    glueMock.on(GetTableCommand).resolves({
      Table: {
        Name: "test_projection",
        DatabaseName: "test_db",
        PartitionKeys: [{ Name: "dt", Type: "string" }],
        Parameters: {
          "projection.enabled": "true",
          "projection.dt.type": "date",
          "projection.dt.format": "yyyy-MM-dd",
          "projection.dt.range": '["2024-01-01","2024-12-31"]',
          "storage.location.template": "s3://bucket/path/${dt}",
        },
      },
    });

    const metadata = await getGlueTableMetadata(glueCli, "test_db", "test_projection");

    expect(metadata.projectionPatterns?.enabled).toBe(true);
    expect(metadata.projectionPatterns?.patterns.dt).toBeDefined();
    expect(metadata.projectionPatterns?.patterns.dt.type).toBe("date");
    expect(glueMock.calls().length).toBe(1); // Only GetTable, no GetPartitions
  });

  it("should fetch and cache standard partitioned table", async () => {
    // Mock Glue response for standard partitioned table
    glueMock.on(GetTableCommand).resolves({
      Table: {
        Name: "test_table",
        DatabaseName: "test_db",
        PartitionKeys: [
          { Name: "year", Type: "string" },
          { Name: "month", Type: "string" },
        ],
        Parameters: {},
      },
    });

    glueMock.on(GetPartitionsCommand).resolves({
      Partitions: [
        {
          Values: ["2024", "01"],
          StorageDescriptor: {
            Location: "s3://bucket/path/year=2024/month=01",
          },
        },
        {
          Values: ["2024", "02"],
          StorageDescriptor: {
            Location: "s3://bucket/path/year=2024/month=02",
          },
        },
      ],
    });

    // First call should hit AWS
    const metadata1 = await getGlueTableMetadata(glueCli, "test_db", "test_table");
    expect(metadata1.table.Name).toBe("test_table");
    expect(metadata1.partitionMetadata?.values.length).toBe(2);
    expect(glueMock.calls().length).toBe(2); // GetTable + GetPartitions
  });
  it("should handle error when loading partitions", async () => {
    glueMock.on(GetTableCommand).resolves({
      Table: {
        Name: "test_error",
        DatabaseName: "test_db",
        PartitionKeys: [{ Name: "dt", Type: "string" }],
        Parameters: {},
      },
    });

    glueMock.on(GetPartitionsCommand).rejects(new Error("Failed to get partitions"));

    const metadata = await getGlueTableMetadata(glueCli, "test_db", "test_error");
    expect(metadata.partitionMetadata).toEqual({ keys: [], values: [] });
  });

  it("should handle different projection types", async () => {
    glueMock.on(GetTableCommand).resolves({
      Table: {
        Name: "test_projections",
        DatabaseName: "test_db",
        PartitionKeys: [
          { Name: "year", Type: "string" },
          { Name: "category", Type: "string" },
          { Name: "id", Type: "string" },
        ],
        Parameters: {
          "projection.enabled": "true",
          "projection.year.type": "integer",
          "projection.year.range": "[2020,2024]",
          "projection.category.type": "enum",
          "projection.category.values": '["books","movies","music"]',
          "projection.id.type": "injected",
        },
      },
    });

    const metadata = await getGlueTableMetadata(glueCli, "test_db", "test_projections");

    expect(metadata.projectionPatterns?.patterns.year.type).toBe("integer");
    expect(metadata.projectionPatterns?.patterns.category.type).toBe("enum");
    expect(metadata.projectionPatterns?.patterns.category.values).toEqual([
      "books",
      "movies",
      "music",
    ]);
    expect(metadata.projectionPatterns?.patterns.id.type).toBe("injected");
  });

  it("should parse comma-separated range values", async () => {
    glueMock.on(GetTableCommand).resolves({
      Table: {
        Name: "test_ranges",
        DatabaseName: "test_db",
        PartitionKeys: [{ Name: "month", Type: "string" }],
        Parameters: {
          "projection.enabled": "true",
          "projection.month.type": "integer",
          "projection.month.range": "1,2,3,4,5,6,7,8,9,10,11,12",
        },
      },
    });

    const metadata = await getGlueTableMetadata(glueCli, "test_db", "test_ranges");
    expect(metadata.projectionPatterns?.patterns.month.range).toEqual([
      "1",
      "2",
      "3",
      "4",
      "5",
      "6",
      "7",
      "8",
      "9",
      "10",
      "11",
      "12",
    ]);
  });

  it("should throw error for missing table", async () => {
    glueMock.on(GetTableCommand).resolves({});

    await expect(getGlueTableMetadata(glueCli, "test_db", "nonexistent")).rejects.toThrow(
      "Table test_db.nonexistent not found"
    );
  });

  describe("getPartitionExtractor", () => {
    it("should handle date projection patterns", async () => {
      const metadata: CachedGlueTableMetadata = {
        timestamp: Date.now(),
        table: {} as any,
        tableType: ETableType.UNPARTITIONED,
        projectionPatterns: {
          enabled: true,
          patterns: {
            dt: {
              type: "date",
              format: "yyyy-MM-dd",
            },
          },
        },
      };

      const extractor = await getPartitionExtractor("dt", metadata);
      expect(extractor).toBe("regexp_extract(path, '(\\d{4}-\\d{2}-\\d{2})', 1)");
    });

    it("should handle integer projection patterns", async () => {
      const metadata: CachedGlueTableMetadata = {
        timestamp: Date.now(),
        table: {} as any,
        tableType: ETableType.UNPARTITIONED,
        projectionPatterns: {
          enabled: true,
          patterns: {
            year: {
              type: "integer",
              range: [2020, 2024],
            },
          },
        },
      };

      const extractor = await getPartitionExtractor("year", metadata);
      expect(extractor).toBe("CAST(regexp_extract(path, '/([0-9]+)/', 1) AS INTEGER)");
    });

    it("should handle enum projection patterns", async () => {
      const metadata: CachedGlueTableMetadata = {
        timestamp: Date.now(),
        table: {} as any,
        tableType: ETableType.UNPARTITIONED,
        projectionPatterns: {
          enabled: true,
          patterns: {
            category: {
              type: "enum",
              values: ["books", "movies"],
            },
          },
        },
      };

      const extractor = await getPartitionExtractor("category", metadata);
      expect(extractor).toBe("regexp_extract(path, '/([^/]+)/[^/]*$', 1)");
    });

    it("should throw error for injected projection patterns", async () => {
      const metadata: CachedGlueTableMetadata = {
        timestamp: Date.now(),
        table: {} as any,
        tableType: ETableType.UNPARTITIONED,
        projectionPatterns: {
          enabled: true,
          patterns: {
            id: {
              type: "injected",
            },
          },
        },
      };

      await expect(getPartitionExtractor("id", metadata)).rejects.toThrow(
        "Injected partition values not supported yet"
      );
    });

    it("should throw error for unknown projection type", async () => {
      const metadata: CachedGlueTableMetadata = {
        timestamp: Date.now(),
        table: {} as any,
        tableType: ETableType.UNPARTITIONED,
        projectionPatterns: {
          enabled: true,
          patterns: {
            test: {
              type: "unknown" as any,
            },
          },
        },
      };

      await expect(getPartitionExtractor("test", metadata)).rejects.toThrow(
        "Unsupported projection type: unknown"
      );
    });

    it("should throw error for missing projection pattern", async () => {
      const metadata: CachedGlueTableMetadata = {
        timestamp: Date.now(),
        table: {} as any,
        tableType: ETableType.UNPARTITIONED,
        projectionPatterns: {
          enabled: true,
          patterns: {},
        },
      };

      await expect(getPartitionExtractor("missing", metadata)).rejects.toThrow(
        "No projection pattern found for partition key missing"
      );
    });

    it("should default to Hive-style partitioning when no projection", async () => {
      const metadata: CachedGlueTableMetadata = {
        timestamp: Date.now(),
        table: {} as any,
        tableType: ETableType.UNPARTITIONED,
      };

      const extractor = await getPartitionExtractor("year", metadata);
      expect(extractor).toBe("regexp_extract(path, 'year=([^/]+)', 1)");
    });
  });

  describe("parseProjectionPatterns", () => {
    it("should handle empty parameters", () => {
      const result = parseProjectionPatterns({});
      expect(result).toEqual({ enabled: true, patterns: {} });
    });

    it("should ignore non-projection parameters", () => {
      const result = parseProjectionPatterns({
        "some.other.param": "value",
        "projection.enabled": "true",
      });
      expect(result).toEqual({ enabled: true, patterns: {} });
    });

    it("should throw on malformed JSON values", () => {
      const params = {
        "projection.enabled": "true",
        "projection.dt.type": "date",
        "projection.dt.values": "{malformed json}",
      };

      expect(() => parseProjectionPatterns(params)).toThrow();
    });
  });
});
