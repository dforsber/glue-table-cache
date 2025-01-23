"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.BaseTableCache = void 0;
const node_api_1 = require("@duckdb/node-api");
const s3_js_1 = require("./util/s3.js");
const iceberg_js_1 = require("./util/iceberg.js");
const client_s3_1 = require("@aws-sdk/client-s3");
const lru_cache_1 = require("lru-cache");
const async_mutex_1 = require("async-mutex");
const async_retry_1 = __importDefault(require("async-retry"));
const debug_js_1 = require("./util/debug.js");
const log = (0, debug_js_1.debug)("base-table-cache");
const logAws = (0, debug_js_1.debug)("base-table-cache:aws");
class BaseTableCache {
    db;
    config;
    s3ListingCache;
    s3Client;
    constructor(config = {}) {
        this.config = {
            region: config.region || process.env.AWS_REGION || "eu-west-1",
            maxEntries: config.maxEntries || 100,
            s3ListingRefresTtlhMs: config.s3ListingRefresTtlhMs || 3600000,
            ...config,
        };
        this.s3ListingCache =
            config.s3ListingCache ??
                new lru_cache_1.LRUCache({
                    max: config.maxEntries || 100,
                    ttl: config.s3ListingRefresTtlhMs || 3600000,
                });
        if (this.config.credentials) {
            this.s3Client = new client_s3_1.S3Client({
                region: this.config.region,
                credentials: this.config.credentials,
            });
        }
        if (this.config.proxyAddress) {
            try {
                new URL(this.config.proxyAddress);
                if (!this.config.proxyAddress.endsWith("/")) {
                    this.config.proxyAddress = this.config.proxyAddress + "/";
                }
                log("Using proxyAddress:", this.config.proxyAddress);
            }
            catch (err) {
                console.error(err);
                this.config.proxyAddress = undefined;
            }
        }
    }
    close() {
        this.db?.close();
        this.db = undefined;
    }
    clearCache() {
        this.s3ListingCache.clear();
    }
    setCredentials(credentials) {
        log("Setting credentials -- accessKeyId:", credentials.accessKeyId);
        if (credentials.secretAccessKey.length <= 0)
            throw new Error("No secretAccessKey");
        this.config.credentials = credentials;
        this.s3Client = new client_s3_1.S3Client({
            region: this.config.region,
            credentials: this.config.credentials,
        });
    }
    async __connect() {
        if (!this.db)
            this.db = await (await node_api_1.DuckDBInstance.create(":memory:")).connect();
        if (!this.db)
            throw new Error("Could not create DuckDB instance");
        if (this.config.credentials?.accessKeyId && this.config.credentials?.secretAccessKey) {
            const { accessKeyId, secretAccessKey, sessionToken } = this.config.credentials;
            log("Using configured credentials for DuckDB");
            await this.__runAndReadAll(`CREATE SECRET s3SecretForIcebergFromCreds (
              TYPE S3,
              KEY_ID '${accessKeyId}',
              SECRET '${secretAccessKey}',
              ${sessionToken ? `SESSION_TOKEN '${sessionToken}',` : ""}
              REGION '${this.config.region}'
          );`);
        }
        else {
            log("Using default credentials chain provider for DuckDB");
            await this.__runAndReadAll(`CREATE OR REPLACE SECRET s3SecretForIcebergWithProvider ( TYPE S3, PROVIDER CREDENTIAL_CHAIN );`);
        }
        return this.db;
    }
    async __runAndReadAll(query) {
        if (!this.db)
            await this.__connect();
        if (!this.db)
            throw new Error("DB not connected");
        return this.db.runAndReadAll(query);
    }
    getCacheKeyWithMutex(cache, key) {
        let cached = cache.get(key);
        if (!cached) {
            cache.set(key, {
                mutex: new async_mutex_1.Mutex(),
                timestamp: Date.now(),
                data: undefined,
            });
            cached = cache.get(key);
        }
        if (!cached)
            throw new Error("Cache initialization failed");
        return cached;
    }
    async __listS3IcebergFilesCached(s3Path, partitionKeys) {
        const key = `${s3Path}:${partitionKeys.join(",")}`;
        const cached = this.getCacheKeyWithMutex(this.s3ListingCache, key);
        if (!cached)
            throw new Error("Could not initialise cache entry");
        if (cached.error)
            delete cached.error;
        if (cached.data) {
            logAws("Using cached S3 (Iceberg) listing for %s", s3Path);
            return cached.data;
        }
        else {
            return cached.mutex.runExclusive(async () => (0, async_retry_1.default)(async (bail, attempt) => {
                if (cached.data)
                    return cached.data;
                if (cached.error) {
                    bail(cached.error);
                    return;
                }
                try {
                    const sql = (0, iceberg_js_1.getIcebergS3FilesStmts)(s3Path);
                    logAws(sql);
                    if (!this.db)
                        await this.__connect();
                    if (!this.db)
                        throw new Error("Could not create db connection");
                    const last = sql.pop();
                    if (!last)
                        throw new Error("No SQL statements generated");
                    for await (const stmt of sql) {
                        log(stmt);
                        await this.db.runAndReadAll(stmt);
                    }
                    const res = (await this.db.runAndReadAll(last)).getRows();
                    const listing = (0, s3_js_1.mapS3PathsToInfo)(res.flat(), partitionKeys);
                    logAws("Listing Iceberg S3 files for %s", { s3Path, partitionKeys, listing });
                    this.s3ListingCache.set(key, { ...cached, timestamp: Date.now(), data: listing });
                    cached.error = undefined;
                    return listing;
                }
                catch (error) {
                    cached.error = error;
                    log("__listS3IcebergFilesCached ERROR:", attempt, error);
                    if (error?.$metadata?.httpStatusCode === 403 ||
                        error?.[0]?.$metadata?.httpStatusCode === 403 ||
                        error?.$metadata?.httpStatusCode === 400 ||
                        error?.[0]?.$metadata?.httpStatusCode === 400 ||
                        error?.message.includes("HTTP 40")) {
                        bail(error);
                        return;
                    }
                    throw error;
                }
            }, {
                retries: 3,
                minTimeout: 200,
                maxTimeout: 500,
                onRetry: (e, a) => log("__listS3IcebergFilesCached -- retry:", a, e),
            }));
        }
    }
    async __listS3FilesCached(s3Path, partitionKeys) {
        if (!this.s3Client)
            throw new Error("No S3 client available");
        const key = `${s3Path}:${partitionKeys.join(",")}`;
        const cached = this.getCacheKeyWithMutex(this.s3ListingCache, key);
        if (!cached)
            throw new Error("Could not initialise cache entry");
        if (cached.error)
            delete cached.error;
        if (cached.data) {
            logAws("Using cached S3 listing for %s", s3Path);
            return cached.data;
        }
        else {
            return cached.mutex.runExclusive(async () => (0, async_retry_1.default)(async (bail, attempt) => {
                if (cached.data)
                    return cached.data;
                if (cached.error) {
                    bail(cached.error);
                    return;
                }
                try {
                    logAws("Listing S3 files for %s", s3Path);
                    if (!this.s3Client)
                        throw new Error("No S3 client available");
                    const files = await (0, s3_js_1.listS3Objects)(this.s3Client, s3Path, partitionKeys);
                    this.s3ListingCache.set(key, { ...cached, timestamp: Date.now(), data: files });
                    return files;
                }
                catch (error) {
                    log("__listS3FilesCached ERROR:", attempt, error);
                    if (error?.$metadata?.httpStatusCode === 403 ||
                        error?.[0]?.$metadata?.httpStatusCode === 403 ||
                        error?.$metadata?.httpStatusCode === 400 ||
                        error?.[0]?.$metadata?.httpStatusCode === 400 ||
                        error?.message.includes("HTTP 40")) {
                        bail(error);
                        return;
                    }
                    throw error;
                }
            }, {
                retries: 3,
                minTimeout: 200,
                maxTimeout: 500,
                onRetry: (e, a) => log("__listS3FilesCached -- retry:", a, e),
            }));
        }
    }
}
exports.BaseTableCache = BaseTableCache;
