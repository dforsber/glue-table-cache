"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.getGlueTableMetadata = getGlueTableMetadata;
exports.parseProjectionPatterns = parseProjectionPatterns;
exports.loadPartitionMetadata = loadPartitionMetadata;
exports.getPartitionExtractor = getPartitionExtractor;
exports.convertDateFormatToRegex = convertDateFormatToRegex;
const client_glue_1 = require("@aws-sdk/client-glue");
async function getGlueTableMetadata(gluecli, DatabaseName, Name) {
    const tableRequest = { DatabaseName, Name };
    const tableResponse = await gluecli.send(new client_glue_1.GetTableCommand(tableRequest));
    const table = tableResponse.Table;
    if (!table)
        throw new Error(`Table ${DatabaseName}.${Name} not found`);
    const metadata = { timestamp: Date.now(), table: table };
    // Handle partition projection if enabled
    if (table.Parameters?.["projection.enabled"] === "true") {
        metadata.projectionPatterns = parseProjectionPatterns(table.Parameters);
    }
    else if (table.PartitionKeys && table.PartitionKeys.length > 0) {
        // Load partition metadata for standard partitioned tables
        metadata.partitionMetadata = await loadPartitionMetadata(gluecli, DatabaseName, Name);
    }
    return metadata;
}
function parseProjectionValue(property, value) {
    switch (property) {
        case "type":
            return value;
        case "format":
            return value;
        case "range":
            // Handle both JSON array format and comma-separated format
            try {
                return JSON.parse(value);
            }
            catch {
                return value.split(",").map((v) => v.trim());
            }
        case "values":
            return JSON.parse(value);
        default:
            return value;
    }
}
function parseProjectionPatterns(parameters) {
    const patterns = {};
    Object.entries(parameters)
        .filter(([key]) => key.startsWith("projection."))
        .forEach(([key, value]) => {
        const match = key.match(/projection\.(\w+)\.(type|range|format|values)/);
        if (match) {
            const [_, column, property] = match;
            if (!patterns[column]) {
                patterns[column] = { type: "enum" };
            }
            if (property === "type" ||
                property === "format" ||
                property === "range" ||
                property === "values") {
                patterns[column][property] =
                    parseProjectionValue(property, value);
            }
        }
    });
    return {
        enabled: true,
        patterns,
    };
}
async function loadPartitionMetadata(gluecli, DatabaseName, TableName) {
    // Implementation for loading standard partition metadata
    const command = new client_glue_1.GetPartitionsCommand({
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
            values: response.Partitions.map((p) => ({
                values: p.Values || [],
                location: p.StorageDescriptor?.Location,
            })) || [],
        };
    }
    catch (error) {
        console.warn(`Failed to load partitions for ${DatabaseName}_${TableName}:`, error);
        return { keys: [], values: [] };
    }
}
async function getPartitionExtractor(key, metadata) {
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
function convertDateFormatToRegex(format) {
    // Convert Java SimpleDateFormat patterns to regex patterns
    const conversions = {
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
