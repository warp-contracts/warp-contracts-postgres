import {
  BasicSortKeyCache,
  Benchmark,
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
  private readonly benchmarkLogger = LoggerFactory.INST.create(PgContractCache.name + 'Benchmark');

  private readonly pool: Pool;
  private client: PoolClient;
  private clientBenchmark: Benchmark;

  constructor(private readonly pgCacheOptions?: PgContractCacheOptions) {
    if (!pgCacheOptions) {
      this.pgCacheOptions = {
        minEntriesPerContract: 10,
        maxEntriesPerContract: 100,
        validityBatchSize: 1000
      };
    }
    if (!pgCacheOptions.validityBatchSize) {
      pgCacheOptions.validityBatchSize = 1000;
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
    this.clientBenchmark = Benchmark.measure();
    await this.client.query('BEGIN;');
  }

  async close(): Promise<void> {
    if (this.client) {
      this.client.release();
    }
    await this.pool.end();
    this.logger.info(`Connection closed`);
    return;
  }

  async commit(): Promise<void> {
    await this.client.query('COMMIT;');
    if (this.clientBenchmark != null) {
      this.clientBenchmark.stop();
      this.benchmarkLogger.debug('PG Benchmark', {
        commit: this.clientBenchmark.elapsed()
      });
      this.clientBenchmark = null;
    }
  }

  async delete(key: string): Promise<void> {
    const delBenchmark = Benchmark.measure();
    await this.connection().query('DELETE FROM warp.sort_key_cache WHERE key = $1;', [key]);
    delBenchmark.stop();
    this.benchmarkLogger.debug('PG Benchmark', {
      delete: delBenchmark.elapsed(),
      'key   ': key
    });
  }

  dump(): Promise<any> {
    return Promise.resolve(undefined);
  }

  async get(cacheKey: CacheKey): Promise<SortKeyCacheResult<EvalStateResult<V>> | null> {
    const getBenchmark = Benchmark.measure();
    const result = await this.connection().query(
      `SELECT value
       FROM warp.sort_key_cache
       WHERE key = $1
         AND sort_key = $2;`,
      [cacheKey.key, cacheKey.sortKey]
    );

    getBenchmark.stop();
    this.benchmarkLogger.debug('PG Benchmark', {
      'get     ': getBenchmark.elapsed(),
      cacheKey: cacheKey
    });
    if (result && result.rows.length > 0) {
      return new SortKeyCacheResult<EvalStateResult<V>>(
        cacheKey.sortKey,
        new EvalStateResult(result.rows[0].value, {}, {})
      );
    }
    return null;
  }

  async getValidityAll(cacheKey: CacheKey): Promise<PgContractValidity> {
    const getBenchmark = Benchmark.measure();
    const result = await this.connection().query(
      `WITH validity_page AS
                (SELECT tx_id, valid, error_message
                 from warp.validity
                 where key = $1
                   and sort_key <= $2
                 ORDER BY sort_key DESC, id DESC)
       select json_object_agg(tx_id, valid) as v, json_object_agg(tx_id, error_message) as err, count(*) as count
       from validity_page;`,
      [cacheKey.key, cacheKey.sortKey]
    );

    getBenchmark.stop();
    this.benchmarkLogger.debug('PG Benchmark', {
      'getValidityAll ': getBenchmark.elapsed(),
      'cacheKey       ': cacheKey
    });

    if (result && result.rows.length > 0) {
      return new PgContractValidity(result.rows[0].v, result.rows[0].err, result.rows[0].count);
    }
    return new PgContractValidity({}, {}, 0);
  }

  async getLast(key: string): Promise<SortKeyCacheResult<EvalStateResult<V>> | null> {
    const getLastBenchmark = Benchmark.measure();
    const result = await this.connection().query(
      'SELECT sort_key, value FROM warp.sort_key_cache WHERE key = $1 ORDER BY sort_key DESC LIMIT 1',
      [key]
    );

    getLastBenchmark.stop();
    this.benchmarkLogger.debug('PG Benchmark', {
      getLast: getLastBenchmark.elapsed(),
      'key    ': key
    });
    if (result && result.rows && result.rows.length > 0) {
      return new SortKeyCacheResult<EvalStateResult<V>>(
        result.rows[0].sort_key,
        new EvalStateResult<V>(result.rows[0].value, {}, {})
      );
    }
    return null;
  }

  async getLastSortKey(): Promise<string | null> {
    const getBenchmark = Benchmark.measure();
    const result = await this.connection().query('SELECT max(sort_key) as sort_key FROM warp.sort_key_cache');
    getBenchmark.stop();
    this.benchmarkLogger.debug('PG Benchmark', {
      getLastSortKey: getBenchmark.elapsed()
    });
    return result.rows[0].sort_key == '' ? null : result.rows[0].sortKey;
  }

  async getLessOrEqual(key: string, sortKey: string): Promise<SortKeyCacheResult<EvalStateResult<V>> | null> {
    const getBenchmark = Benchmark.measure();
    const result = await this.connection().query(
      'SELECT sort_key, value FROM warp.sort_key_cache WHERE key = $1 AND sort_key <= $2 ORDER BY sort_key DESC LIMIT 1',
      [key, sortKey]
    );

    getBenchmark.stop();
    this.benchmarkLogger.debug('PG Benchmark', {
      getLessOrEqual: getBenchmark.elapsed(),
      'key           ': key,
      'sortKey       ': sortKey
    });
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
    const openBenchmark = Benchmark.measure();
    this.client = await this.pool.connect();
    await this.client.query("CREATE schema if not exists warp; SET search_path TO 'warp';");
    await this.sortKeyTable();
    await this.validityTable();
    this.logger.info(`Connected`);
    openBenchmark.stop();
    this.benchmarkLogger.debug('PG Benchmark', {
      open: openBenchmark.elapsed()
    });
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
    const pruneBenchmark = Benchmark.measure();
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

    pruneBenchmark.stop();
    this.benchmarkLogger.debug('PG Benchmark', {
      prune: pruneBenchmark.elapsed()
    });
    return {
      entriesBefore: allItems,
      entriesAfter: allItems - deleted,
      sizeBefore: -1,
      sizeAfter: -1
    };
  }

  async put(stateCacheKey: CacheKey, value: EvalStateResult<V>): Promise<void> {
    const putBenchmark = Benchmark.measure();

    const stringifyBenchmark = Benchmark.measure();
    const stringifiedState = JSON.stringify(value.state);
    stringifyBenchmark.stop();
    this.benchmarkLogger.debug('PG Benchmark', {
      stringify: stringifyBenchmark.elapsed()
    });

    const rmBenchmark = Benchmark.measure();
    await this.removeOldestEntries(stateCacheKey.key);
    rmBenchmark.stop();
    this.benchmarkLogger.debug('PG Benchmark', {
      'remove   ': rmBenchmark.elapsed()
    });

    const insertBenchmark = Benchmark.measure();
    await this.connection().query(
      `
                INSERT INTO warp.sort_key_cache (key, sort_key, value)
                VALUES ($1, $2, $3)
                ON CONFLICT(key, sort_key) DO UPDATE SET value = EXCLUDED.value`,
      [stateCacheKey.key, stateCacheKey.sortKey, stringifiedState]
    );

    let sql = '';
    let batchCounter = 0;
    let valueCounter = 0;
    let values = [];
    for (const [tx, v] of Object.entries(value.validity)) {
      batchCounter++;
      values.push(stateCacheKey.key, stateCacheKey.sortKey, tx, v, value.errorMessages[tx]);
      sql += `${batchCounter > 1 ? ',' : ''} ($${++valueCounter}, $${++valueCounter}, $${++valueCounter}, $${++valueCounter}, $${++valueCounter})`;
      if (batchCounter % this.pgCacheOptions.validityBatchSize === 0) {
        await this.queryInsertValidity(sql, values);
        sql = '';
        batchCounter = 0;
        valueCounter = 0;
        values = [];
      }
    }
    await this.queryInsertValidity(sql, values);

    insertBenchmark.stop();
    this.benchmarkLogger.debug('PG Benchmark', {
      'insert   ': insertBenchmark.elapsed()
    });

    putBenchmark.stop();
    this.benchmarkLogger.debug('PG Benchmark', {
      'put          ': putBenchmark.elapsed(),
      stateCacheKey: stateCacheKey
    });
  }

  private async queryInsertValidity(sql: string, values: any[]): Promise<void> {
    if (sql && values) {
      await this.connection().query(
        `INSERT INTO warp.validity (key, sort_key, tx_id, valid, error_message)
        VALUES
        ${sql} 
        ON CONFLICT(key, tx_id)
        DO UPDATE
        SET valid = EXCLUDED.valid,
                    sort_key      = EXCLUDED.sort_key,
                    error_message = EXCLUDED.error_message`,
        values
      );
    }
  }

  private async removeOldestEntries(key: string) {
    const rs = await this.connection().query(
      `
          SELECT count(1) as total
          FROM warp.sort_key_cache
          WHERE key = $1;
      `,
      [key]
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
    const signatureBenchmark = Benchmark.measure();

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
    signatureBenchmark.stop();
    this.benchmarkLogger.debug('PG Benchmark', {
      setSignature: signatureBenchmark.elapsed(),
      'cacheKey    ': cacheKey
    });
  }

  async rollback(): Promise<void> {
    await this.client.query('ROLLBACK;');
    if (this.clientBenchmark != null) {
      this.clientBenchmark.stop();
      this.benchmarkLogger.debug('PG Benchmark', {
        rollback: this.clientBenchmark.elapsed()
      });
      this.clientBenchmark = null;
    }
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

export class PgContractValidity {
  constructor(v: Record<string, boolean>, err: Record<string, string>, count: number) {
    this.validity = v;
    this.errorMessages = err;
    this.count = count;
  }

  readonly validity: Record<string, boolean>;
  readonly errorMessages: Record<string, string>;
  readonly count: number;
}
