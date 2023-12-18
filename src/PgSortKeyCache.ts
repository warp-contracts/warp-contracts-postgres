import {
  BatchDBOp,
  Benchmark,
  CacheKey,
  LoggerFactory,
  PruneStats,
  SortKeyCache,
  SortKeyCacheResult
} from 'warp-contracts';
import { Pool, PoolClient } from 'pg';
import { SortKeyCacheRangeOptions } from 'warp-contracts/lib/types/cache/SortKeyCacheRangeOptions';
import { PgSortKeyCacheOptions } from './PgSortKeyCacheOptions';

export class PgSortKeyCache<V> implements SortKeyCache<V> {
  private readonly logger = LoggerFactory.INST.create(PgSortKeyCache.name);
  private readonly benchmarkLogger = LoggerFactory.INST.create(PgSortKeyCache.name + 'Benchmark');

  private readonly tableName: string;
  private readonly schemaName: string;
  private setupPerformed = false;
  private pool: Pool;
  private client: PoolClient;
  private clientBenchmark: Benchmark;

  constructor(private readonly pgCacheOptions: PgSortKeyCacheOptions) {
    if (!pgCacheOptions.schemaName) {
      throw new Error('Schema name cannot be empty');
    }
    if (!pgCacheOptions.tableName) {
      throw new Error('Table name cannot be empty');
    }
    this.schemaName = pgCacheOptions.schemaName;
    this.tableName = pgCacheOptions.tableName;
    this.pool = new Pool(pgCacheOptions);
  }

  private async createTableIfNotExists() {
    await this.connection().query(
      `CREATE schema if not exists "${this.schemaName}"; SET search_path TO '${this.schemaName}';`
    );
    this.logger.info(`Attempting to create table ${this.tableName}`);
    const query = `
          CREATE TABLE IF NOT EXISTS "${this.tableName}"
          (
              id            bigserial,
              key           TEXT NOT NULL,
              sort_key      TEXT NOT NULL,
              value         JSONB,
              PRIMARY KEY (key, sort_key)
          );
          CREATE INDEX IF NOT EXISTS "idx_${this.tableName}_key_sk" ON "${this.tableName}" (key, sort_key DESC);
          CREATE INDEX IF NOT EXISTS "idx_${this.tableName}_key" ON "${this.tableName}" (key);`;
    await this.connection().query(query);
  }

  async begin(): Promise<void> {
    this.clientBenchmark = Benchmark.measure();
    this.logger.debug(`Begin transaction`);
    if (this.client == null) {
      this.client = await this.pool.connect();
    }
    await this.client.query('BEGIN;');
  }

  async close(): Promise<void> {
    if (this.client) {
      this.client.release();
      this.client = null;
      this.logger.info(`Connection released back to the pool`);
    }
    return;
  }

  async cleanUp(): Promise<void> {
    await this.close();
    await this.pool.end();
    this.pool = null;
    this.logger.info(`Pool cleaned up`);
  }

  async commit(): Promise<void> {
    this.logger.debug(`Commit transaction`);
    if (this.client == null) {
      this.logger.error(`Called commit when no connection established.`);
      return;
    }
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
    await this.connection().query(`DELETE FROM "${this.schemaName}"."${this.tableName}" WHERE key = $1;`, [key]);
    delBenchmark.stop();
    this.benchmarkLogger.debug('PG Benchmark', {
      delete: delBenchmark.elapsed(),
      'key   ': key
    });
  }

  dump(): Promise<any> {
    return Promise.resolve(undefined);
  }

  async get(cacheKey: CacheKey): Promise<SortKeyCacheResult<V> | null> {
    const getBenchmark = Benchmark.measure();
    const result = await this.connection().query(
      `SELECT value
       FROM "${this.schemaName}"."${this.tableName}"
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
      return new SortKeyCacheResult<V>(cacheKey.sortKey, result.rows[0].value);
    }
    return null;
  }

  async getLast(key: string): Promise<SortKeyCacheResult<V> | null> {
    const getLastBenchmark = Benchmark.measure();
    const result = await this.connection().query(
      `SELECT sort_key, value FROM "${this.schemaName}"."${this.tableName}" WHERE key = $1 ORDER BY sort_key DESC LIMIT 1;`,
      [key]
    );

    getLastBenchmark.stop();
    this.logger.info('PG Benchmark', {
      getLast: getLastBenchmark.elapsed(),
      'key    ': key
    });
    if (result && result.rows && result.rows.length > 0) {
      return new SortKeyCacheResult<V>(result.rows[0].sort_key, result.rows[0].value);
    }
    return null;
  }

  async getLastSortKey(): Promise<string | null> {
    const getBenchmark = Benchmark.measure();
    const result = await this.connection().query(
      `SELECT max(sort_key) as sort_key FROM "${this.schemaName}"."${this.tableName}";`
    );
    getBenchmark.stop();
    this.benchmarkLogger.debug('PG Benchmark', {
      getLastSortKey: getBenchmark.elapsed()
    });
    return result.rows[0].sort_key == '' ? null : result.rows[0].sortKey;
  }

  async getLessOrEqual(key: string, sortKey: string): Promise<SortKeyCacheResult<V> | null> {
    const getBenchmark = Benchmark.measure();
    const result = await this.connection().query(
      `SELECT sort_key, value FROM "${this.schemaName}"."${this.tableName}" WHERE key = $1 AND sort_key <= $2 ORDER BY sort_key DESC LIMIT 1;`,
      [key, sortKey]
    );

    getBenchmark.stop();
    this.benchmarkLogger.debug('PG Benchmark', {
      getLessOrEqual: getBenchmark.elapsed(),
      'key           ': key,
      'sortKey       ': sortKey
    });
    if (result && result.rows.length > 0) {
      return new SortKeyCacheResult<V>(result.rows[0].sort_key, result.rows[0].value);
    }
    return null;
  }

  async setUp(): Promise<void> {
    const conf = this.pgCacheOptions;
    this.logger.info(`Connecting pg... ${conf.user}@${conf.host}:${conf.port}/${conf.database}`);
    await this.pool.query(`CREATE schema if not exists "${this.schemaName}"; SET search_path TO "${this.schemaName}";`);
    await this.createTableIfNotExists();
    this.logger.info(`Setup finished`);
  }

  async open(): Promise<void> {
    const openBenchmark = Benchmark.measure();
    if (!this.setupPerformed) {
      await this.setUp();
      this.setupPerformed = true;
    }
    if (!this.client) {
      this.client = await this.pool.connect();
    }
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
         FROM "${this.schemaName}"."${this.tableName}"`
      )
    ).rows[0].total;

    const deleted = +(
      await this.client.query(
        `
            WITH sorted_cache AS
                         (SELECT id, key, sort_key, row_number() over (PARTITION BY "key" ORDER BY sort_key DESC) AS rw
            FROM "${this.schemaName}"."${this.tableName}"), deleted AS
                (
            DELETE
            FROM "${this.schemaName}"."${this.tableName}"
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

  async put(stateCacheKey: CacheKey, value: V): Promise<void> {
    const putBenchmark = Benchmark.measure();
    const stringifiedValue = JSON.stringify(value);
    await this.removeOldestEntries(stateCacheKey.key);

    await this.connection().query(
      `
                INSERT INTO "${this.schemaName}"."${this.tableName}" (key, sort_key, value)
                VALUES ($1, $2, $3)
                ON CONFLICT(key, sort_key) DO UPDATE SET value = EXCLUDED.value`,
      [stateCacheKey.key, stateCacheKey.sortKey, stringifiedValue]
    );

    putBenchmark.stop();
    this.benchmarkLogger.debug('PG Benchmark', {
      'put          ': putBenchmark.elapsed(),
      stateCacheKey: stateCacheKey
    });
  }

  private async removeOldestEntries(key: string) {
    const rs = await this.connection().query(
      `
          SELECT count(1) as total
          FROM "${this.schemaName}"."${this.tableName}"
          WHERE key = $1
      `,
      [key]
    );
    if (rs.rows.length > 0) {
      const entriesTotal = rs.rows[0].total;
      if (entriesTotal >= this.pgCacheOptions.maxEntriesPerKey) {
        await this.connection().query(
          `
          WITH sorted_cache AS
                   (SELECT id, row_number() over (ORDER BY sort_key DESC) AS rw
                    FROM "${this.schemaName}"."${this.tableName}"
                    WHERE key = $1)
          DELETE
          FROM "${this.schemaName}"."${this.tableName}"
          WHERE id IN (SELECT id FROM sorted_cache WHERE rw >= $2);
      `,
          [key, this.pgCacheOptions.minEntriesPerKey]
        );
      }
    }
  }

  async rollback(): Promise<void> {
    this.logger.debug(`Rollback`);
    if (this.client == null) {
      this.logger.error(`Rollback called, but no connection established`);
      return;
    }
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
          DROP INDEX IF EXISTS "idx_${this.tableName}_key_sk";
          DROP INDEX IF EXISTS "idx_${this.tableName}_key";
          DROP INDEX IF EXISTS "idx_${this.tableName}_owner";
          DROP TABLE IF EXISTS "${this.schemaName}"."${this.tableName}";
      `
    );
  }

  async batch(opStack: BatchDBOp<V>[]): Promise<any> {
    try {
      await this.begin();
      for (const op of opStack) {
        if (op.type === 'put') {
          await this.put(op.key, op.value);
        } else if (op.type === 'del') {
          await this.delete(op.key);
        }
      }
      await this.commit();
    } catch (e) {
      await this.rollback();
      throw e;
    } finally {
      this.client.release();
      this.client = null;
    }
  }

  async del(cacheKey: CacheKey): Promise<void> {
    const delBenchmark = Benchmark.measure();
    await this.connection().query(
      `
              INSERT INTO "${this.schemaName}"."${this.tableName}" (key, sort_key, value)
              VALUES ($1, $2, NULL)
              ON CONFLICT(key, sort_key) DO UPDATE SET value = EXCLUDED.value`,
      [cacheKey.key, cacheKey.sortKey]
    );
    delBenchmark.stop();
    this.benchmarkLogger.debug('PG Benchmark', {
      'del     ': delBenchmark.elapsed(),
      cacheKey: cacheKey
    });
    return Promise.resolve(undefined);
  }

  async keys(sortKey: string, options?: SortKeyCacheRangeOptions): Promise<string[]> {
    const keysBenchmark = Benchmark.measure();
    const order = options?.reverse ? 'DESC' : 'ASC';
    const result = await this.connection().query({
      text: `WITH latest_values AS (SELECT DISTINCT ON (key) key, sort_key, value
                                     FROM "${this.schemaName}"."${this.tableName}"
                                     WHERE sort_key <= $1
                                       AND value IS NOT NULL
                                       AND ($2::text IS NULL OR key >= $2)
                                       AND ($3::text IS NULL OR key < $3)
                                     order by key ${order}, sort_key desc
                                     LIMIT $4::bigint)
              select key, value
              from latest_values
              order by key ${order};`,
      values: [sortKey, options?.gte, options?.lt, options?.limit],
      rowMode: 'array'
    });
    keysBenchmark.stop();
    this.benchmarkLogger.debug('PG Benchmark', {
      'keys    ': keysBenchmark.elapsed(),
      'keys len': result.rows.length,
      'sortKey ': sortKey
    });
    return result.rows.flat();
  }

  async kvMap(sortKey: string, options?: SortKeyCacheRangeOptions): Promise<Map<string, V>> {
    const kvBenchmark = Benchmark.measure();
    const order = options?.reverse ? 'DESC' : 'ASC';
    const result = await this.connection().query(
      `
              WITH latest_values AS (SELECT DISTINCT ON (key) key, sort_key, value
                                     FROM "${this.schemaName}"."${this.tableName}"
                                     WHERE sort_key <= $1
                                       AND value IS NOT NULL
                                       AND ($2::text IS NULL OR key >= $2)
                                       AND ($3::text IS NULL OR key < $3)
                                     order by key ${order}, sort_key desc
                                     LIMIT $4::bigint)
              select key, value
              from latest_values
              order by key ${order};`,
      [sortKey, options?.gte, options?.lt, options?.limit]
    );
    const kv = new Map(result.rows.map((i): [string, V] => [i.key, i.value]));
    kvBenchmark.stop();
    this.benchmarkLogger.debug('PG Benchmark', {
      'kv     ': kvBenchmark.elapsed(),
      'kv size': kv.size,
      sortKey: sortKey
    });
    return kv;
  }
}
