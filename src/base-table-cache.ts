import { DuckDBInstance, DuckDBConnection } from "@duckdb/node-api";
import { DuckDBResultReader } from "@duckdb/node-api/lib/DuckDBResultReader.js";
import debug from "debug";

const log = debug("base-table-cache");

export interface BaseTableCacheConfig {
  proxyAddress?: string;
  credentials?: {
    accessKeyId: string;
    secretAccessKey: string;
    sessionToken?: string;
  };
}

export abstract class BaseTableCache {
  protected db: DuckDBConnection | undefined;
  protected config: BaseTableCacheConfig;

  constructor(config: Partial<BaseTableCacheConfig> = {}) {
    this.config = config;
    if (this.config.proxyAddress) {
      try {
        new URL(this.config.proxyAddress);
        if (!this.config.proxyAddress.endsWith("/")) {
          this.config.proxyAddress = this.config.proxyAddress + "/";
        }
        log("Using proxyAddress:", this.config.proxyAddress);
      } catch (err) {
        console.error(err);
        this.config.proxyAddress = undefined;
      }
    }
  }

  public setCredentials(credentials: {
    accessKeyId: string;
    secretAccessKey: string;
    sessionToken?: string;
  }) {
    log("Setting credentials -- accessKeyId:", credentials.accessKeyId);
    if (credentials.secretAccessKey.length <= 0) throw new Error("No secretAccessKey");
    this.config.credentials = credentials;
  }

  protected async __connect(): Promise<DuckDBConnection> {
    if (!this.db) this.db = await (await DuckDBInstance.create(":memory:")).connect();
    if (!this.db) throw new Error("Could not create DuckDB instance");
    await this.configureConnection();
    return this.db;
  }

  protected abstract configureConnection(): Promise<void>;

  public close(): void {
    this.db?.close();
    this.db = undefined;
  }

  protected async __runAndReadAll(query: string): Promise<DuckDBResultReader> {
    if (!this.db) await this.__connect();
    if (!this.db) throw new Error("DB not connected");
    return this.db.runAndReadAll(query);
  }

  public abstract clearCache(): void;
  public abstract convertGlueTableQuery(query: string): Promise<string>;
  public abstract getGlueTableViewSetupSql(query: string): Promise<string[]>;
}
