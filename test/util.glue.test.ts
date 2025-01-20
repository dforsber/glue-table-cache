import { GetPartitionsCommand, GetTableCommand, GlueClient } from "@aws-sdk/client-glue";
import { convertDateFormatToRegex, getGlueTableMetadata } from "../src/util/glue";
import { mockClient } from "aws-sdk-client-mock";

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
});
