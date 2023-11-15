import {
  BasicSortKeyCache,
  CacheKey,
  EvalStateResult,
  LoggerFactory,
  PruneStats,
  SortKeyCacheResult
} from 'warp-contracts';
import { Pool, PoolClient } from 'pg';
import { PgContractCacheOptions } from './PgContractCacheOptions';

export class PgContractCache<V> implements BasicSortKeyCache<EvalStateResult<V>> {
  private readonly logger = LoggerFactory.INST.create(PgContractCache.name);

  private readonly pool: Pool;
  private client: PoolClient;

  constructor(private readonly pgCacheOptions?: PgContractCacheOptions) {
    if (!pgCacheOptions) {
      this.pgCacheOptions = {
        minEntriesPerContract: 10,
        maxEntriesPerContract: 100
      };
    }
    this.pool = new Pool(pgCacheOptions);
  }

  private async sortKeyTable() {
    await this.client.query(
      `
          CREATE TABLE IF NOT EXISTS sort_key_cache
          (
              id            bigserial,
              key           TEXT NOT NULL,
              sort_key      TEXT NOT NULL,
              value         JSONB,
              state_hash    TEXT,
              signature     TEXT,
              PRIMARY KEY (key, sort_key)
          );
          CREATE INDEX IF NOT EXISTS idx_sort_key_cache_key_sk ON sort_key_cache (key, sort_key DESC);
          CREATE INDEX IF NOT EXISTS idx_sort_key_cache_key ON sort_key_cache (key);
          CREATE INDEX IF NOT EXISTS idx_sort_key_cache_owner ON sort_key_cache ((value ->> 'owner'));
      `
    );
  }

  private async validityTable() {
    await this.client.query(
      `
                create table if not exists validity
                (
                    id            BIGSERIAL PRIMARY KEY,
                    sort_key      TEXT    NOT NULL,
                    key           TEXT    NOT NULL,
                    tx_id         TEXT    NOT NULL,
                    valid         BOOLEAN NOT NULL,
                    error_message TEXT DEFAULT NULL,
                    UNIQUE (key, tx_id)
                );
                CREATE INDEX IF NOT EXISTS idx_validity_key_sk ON validity (key, id DESC);
                CREATE INDEX IF NOT EXISTS idx_validity_key ON validity (key);
            `
    );
  }

  async begin(): Promise<void> {
    await this.client.query('BEGIN;');
  }

  async close(): Promise<void> {
    if (this.client) {
      await this.client.release();
    }
    await this.pool.end();
    this.logger.info(`Connection closed`);
    return;
  }

  async commit(): Promise<void> {
    await this.client.query('COMMIT;');
  }

  async delete(key: string): Promise<void> {
    await this.connection().query('DELETE FROM warp.sort_key_cache WHERE key = $1;', [key]);
  }

  dump(): Promise<any> {
    return Promise.resolve(undefined);
  }

  async get(cacheKey: CacheKey): Promise<SortKeyCacheResult<EvalStateResult<V>> | null> {
    const result = await this.connection().query(
      `SELECT value
       FROM warp.sort_key_cache
       WHERE key = $1
         AND sort_key = $2;`,
      [cacheKey.key, cacheKey.sortKey]
    );

    if (result && result.rows.length > 0) {
      return new SortKeyCacheResult<EvalStateResult<V>>(
        cacheKey.sortKey,
        new EvalStateResult(result.rows[0].value, {}, {})
      );
    }
    return null;
  }

  async getLast(key: string): Promise<SortKeyCacheResult<EvalStateResult<V>> | null> {
    const result = await this.connection().query(
      'SELECT sort_key, value FROM warp.sort_key_cache WHERE key = $1 ORDER BY sort_key DESC LIMIT 1',
      [key]
    );

    if (result && result.rows && result.rows.length > 0) {
      return new SortKeyCacheResult<EvalStateResult<V>>(
        result.rows[0].sort_key,
        new EvalStateResult<V>(result.rows[0].value, {}, {})
      );
    }
    return null;
  }

  async getLastSortKey(): Promise<string | null> {
    const result = await this.connection().query('SELECT max(sort_key) as sort_key FROM warp.sort_key_cache');
    return result.rows[0].sort_key == '' ? null : result.rows[0].sortKey;
  }

  async getLessOrEqual(key: string, sortKey: string): Promise<SortKeyCacheResult<EvalStateResult<V>> | null> {
    const result = await this.connection().query(
      'SELECT sort_key, value FROM warp.sort_key_cache WHERE key = $1 AND sort_key <= $2 ORDER BY sort_key DESC LIMIT 1',
      [key, sortKey]
    );

    if (result && result.rows.length > 0) {
      return new SortKeyCacheResult<EvalStateResult<V>>(
        result.rows[0].sort_key,
        new EvalStateResult<V>(result.rows[0].value, {}, {})
      );
    }
    return null;
  }

  async open(): Promise<void> {
    const conf = this.pgCacheOptions;
    this.logger.info(`Connecting pg... ${conf.user}@${conf.host}:${conf.port}/${conf.database}`);
    this.client = await this.pool.connect();
    await this.client.query("CREATE schema if not exists warp; SET search_path TO 'warp';");
    await this.sortKeyTable();
    await this.validityTable();
    this.logger.info(`Connected`);
  }

  private connection(): Pool | PoolClient {
    if (this.client) {
      return this.client;
    }
    return this.pool;
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
      sizeAfter: -1
    };
  }

  async put(stateCacheKey: CacheKey, value: EvalStateResult<V>): Promise<void> {
    const stringifiedState = JSON.stringify(value.state);
    await this.removeOldestEntries(stateCacheKey.key);

    await this.connection().query(
      `
                INSERT INTO warp.sort_key_cache (key, sort_key, value)
                VALUES ($1, $2, $3)
                ON CONFLICT(key, sort_key) DO UPDATE SET value = EXCLUDED.value`,
      [stateCacheKey.key, stateCacheKey.sortKey, stringifiedState]
    );

    for (const tx in value.validity) {
      await this.connection().query(
        `
            INSERT INTO warp.validity (key, sort_key, tx_id, valid, error_message)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT(key, tx_id) DO UPDATE
                SET valid         = EXCLUDED.valid,
                    sort_key      = EXCLUDED.sort_key,
                    error_message = EXCLUDED.error_message`,
        [stateCacheKey.key, stateCacheKey.sortKey, tx, value.validity[tx], value.errorMessages[tx]]
      );
    }
  }

  private async removeOldestEntries(key: string) {
    const rs = await this.connection().query(
      `
          SELECT count(1) as total
          FROM warp.sort_key_cache
          WHERE key = $1;
      `, [key]
    );
    if (rs.rows.length > 0) {
      const entriesTotal = rs.rows[0].total;
      if (entriesTotal >= this.pgCacheOptions.maxEntriesPerContract) {
        await this.connection().query(
          `
          WITH sorted_cache AS
                   (SELECT id, row_number() over (ORDER BY sort_key DESC) AS rw
                    FROM warp.sort_key_cache
                    WHERE key = $1)
          DELETE
          FROM warp.sort_key_cache
          WHERE id IN (SELECT id FROM sorted_cache WHERE rw >= $2);
      `,
          [key, this.pgCacheOptions.minEntriesPerContract]
        );
      }
    }
  }

  /**
   * Executed in a separate pool client, so that in can be used by a separate worker.
   */
  async setSignature(cacheKey: CacheKey, hash: string, signature: string): Promise<void> {
    this.logger.debug(`Attempting to set signature`, cacheKey, signature);
    const result = await this.pool.query(
      `
                    WITH updated AS (
                        UPDATE warp.sort_key_cache
                            SET state_hash = $1,
                                signature = $2
                            WHERE key = $3
                                AND sort_key = $4
                            RETURNING *)
                    SELECT count(*) AS total, array_agg(key) AS keys
                    FROM updated;`,
      [hash, signature, cacheKey.key, cacheKey.sortKey]
    );
    this.logger.debug(`Signature set`, result);
  }

  async rollback(): Promise<void> {
    await this.client.query('ROLLBACK;');
  }

  storage<S>(): S {
    return this.client as S;
  }

  async drop(): Promise<void> {
    await this.client.query(
      `
          DROP INDEX IF EXISTS idx_sort_key_cache_key_sk;
          DROP INDEX IF EXISTS idx_sort_key_cache_key;
          DROP INDEX IF EXISTS idx_sort_key_cache_owner;
          DROP TABLE IF EXISTS sort_key_cache;
          DROP INDEX IF EXISTS idx_validity_key;
          DROP INDEX IF EXISTS idx_validity_key_sk;
          DROP TABLE IF EXISTS validity;
      `
    );
  }
}
