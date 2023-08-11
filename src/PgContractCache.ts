import {
  BasicSortKeyCache,
  CacheKey,
  CacheOptions,
  LoggerFactory,
  PruneStats,
  SortKeyCacheResult,
} from "warp-contracts";
import { Client } from "pg";
import { PgCacheOptions } from "./PgCacheOptions";

export class PgContractCache<V> implements BasicSortKeyCache<V> {
  private readonly logger = LoggerFactory.INST.create(PgContractCache.name);

  private readonly client: Client;

  constructor(
    private readonly cacheOptions: CacheOptions,
    private readonly pgCacheOptions?: PgCacheOptions
  ) {
    if (!pgCacheOptions) {
      this.pgCacheOptions = {
        maxEntriesPerContract: 10,
      };
    }
    this.client = new Client(pgCacheOptions);
  }

  private async sortKeyTable() {
    await this.client.query(
      `
          CREATE TABLE IF NOT EXISTS sort_key_cache
          (
              id       bigserial,
              key      TEXT NOT NULL,
              sort_key TEXT NOT NULL,
              value    JSONB,
              state_hash TEXT, 
              validity_hash TEXT,
              PRIMARY KEY (key, sort_key)
          );
          CREATE INDEX IF NOT EXISTS idx_sort_key_cache_key_sk ON sort_key_cache (key, sort_key DESC);
          CREATE INDEX IF NOT EXISTS idx_sort_key_cache_key ON sort_key_cache (key);
          CREATE INDEX IF NOT EXISTS idx_sort_key_cache_sk ON sort_key_cache (sort_key DESC);
      `
    );
  }

  async begin(): Promise<void> {
    await this.client.query("BEGIN;");
  }

  async close(): Promise<void> {
    await this.client.end();
    this.logger.info(`Connection closed`);
    return;
  }

  async commit(): Promise<void> {
    await this.client.query("COMMIT;");
  }

  async delete(key: string): Promise<void> {
    await this.client.query("DELETE FROM sort_key_cache WHERE key = $1;", [
      key,
    ]);
  }

  dump(): Promise<any> {
    return Promise.resolve(undefined);
  }

  async get(cacheKey: CacheKey): Promise<SortKeyCacheResult<V> | null> {
    const result = await this.client.query(
      `SELECT value
       FROM sort_key_cache
       WHERE key = $1
         AND sort_key = $2;`,
      [cacheKey.key, cacheKey.sortKey]
    );

    if (result && result.rows.length > 0) {
      return new SortKeyCacheResult<V>(cacheKey.sortKey, result.rows[0].value);
    }
    return null;
  }

  async getLast(key: string): Promise<SortKeyCacheResult<V> | null> {
    const result = await this.client.query(
      "SELECT sort_key, value FROM sort_key_cache WHERE key = $1 ORDER BY sort_key DESC LIMIT 1",
      [key]
    );

    if (result && result.rows && result.rows.length > 0) {
      return new SortKeyCacheResult<V>(
        result.rows[0].sort_key,
        result.rows[0].value
      );
    }
    return null;
  }

  async getLastSortKey(): Promise<string | null> {
    const result = await this.client.query(
      "SELECT max(sort_key) as sort_key FROM sort_key_cache"
    );
    return result.rows[0].sort_key == "" ? null : result.rows[0].sortKey;
  }

  async getLessOrEqual(
    key: string,
    sortKey: string
  ): Promise<SortKeyCacheResult<V> | null> {
    const result = await this.client.query(
      "SELECT sort_key, value FROM sort_key_cache WHERE key = $1 AND sort_key <= $2 ORDER BY sort_key DESC LIMIT 1",
      [key, sortKey]
    );

    if (result && result.rows.length > 0) {
      return new SortKeyCacheResult<V>(
        result.rows[0].sort_key,
        result.rows[0].value
      );
    }
    return null;
  }

  async open(): Promise<void> {
    this.logger.info(
      `Connecting pg... ${this.client.user}@${this.client.host}:${this.client.port}/${this.client.database}`
    );
    await this.client.connect();
    await this.sortKeyTable();
    this.logger.info(`Connected`);
  }

  /**
   Let's assume that given contract cache contains these sortKeys: [a, b, c, d, e, f]
   Let's assume entriesStored = 2
   After pruning, the cache should be left with these keys: [e,f].

   const entries = await contractCache.keys({ reverse: true, limit: entriesStored }).all();
   This would return in this case entries [f, e] (notice the "reverse: true").

   await contractCache.clear({ lt: entries[entries.length - 1] });
   This effectively means: await contractCache.clear({ lt: e });
   -> hence the entries [a,b,c,d] are removed and left are the [e,f]
   */
  async prune(entriesStored = 5): Promise<PruneStats> {
    if (!entriesStored || entriesStored <= 0) {
      entriesStored = 1;
    }

    const allItems = +(
      await this.client.query(
        `SELECT count(1) AS total
         FROM sort_key_cache`
      )
    ).rows[0].total;

    const deleted = +(
      await this.client.query(
        `
            WITH sorted_cache AS
                         (SELECT id, key, sort_key, row_number() over (PARTITION BY "key" ORDER BY sort_key DESC) AS rw
            FROM sort_key_cache), deleted AS
                (
            DELETE
            FROM sort_key_cache
            WHERE id IN (SELECT id FROM sorted_cache WHERE rw > $1) RETURNING *)
            SELECT count(1) AS del_total
            FROM deleted;
        `,
        [entriesStored]
      )
    ).rows[0].del_total;

    return {
      entriesBefore: allItems,
      entriesAfter: allItems - deleted,
      sizeBefore: -1,
      sizeAfter: -1,
    };
  }

  async put(stateCacheKey: CacheKey, value: V): Promise<void> {
    await this.removeOldestEntries(stateCacheKey.key);
    await this.client.query(
      "INSERT INTO sort_key_cache (key, sort_key, value) VALUES ($1, $2, $3) ON CONFLICT(key, sort_key) DO UPDATE SET value = EXCLUDED.value",
      [stateCacheKey.key, stateCacheKey.sortKey, value]
    );
  }

  private async removeOldestEntries(key: string) {
    await this.client.query(
      `
          WITH sorted_cache AS
                   (SELECT id, row_number() over (ORDER BY sort_key DESC) AS rw
                    FROM sort_key_cache
                    WHERE key = $1)
          DELETE
          FROM sort_key_cache
          WHERE id IN (SELECT id FROM sorted_cache WHERE rw >= $2);
      `,
      [key, this.pgCacheOptions.maxEntriesPerContract]
    );
  }

  async rollback(): Promise<void> {
    await this.client.query("BEGIN;");
  }

  storage<S>(): S {
    return this.client as S;
  }

  async drop(): Promise<void> {
    await this.client.query(
      `
          DROP INDEX IF EXISTS idx_sort_key_cache_key_sk;
          DROP INDEX IF EXISTS idx_sort_key_cache_sk;
          DROP INDEX IF EXISTS idx_sort_key_cache_key;
          DROP TABLE IF EXISTS sort_key_cache;
      `
    );
  }
}
