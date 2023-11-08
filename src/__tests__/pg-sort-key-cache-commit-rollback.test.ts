import { CacheKey } from 'warp-contracts';
import { getSortKey, sortKeyCache } from './utils';
import { PgSortKeyCache } from '../PgSortKeyCache';

let sut: PgSortKeyCache<unknown>;

beforeAll(async () => {
  sut = await sortKeyCache(100);
});

describe('Postgres cache batch', () => {
  it('access range keys during active transaction and commit', async () => {
    await sut.open();
    const sortKey = 343;

    await sut.begin();
    await sut.put(new CacheKey('key.one', getSortKey(sortKey)), 1);
    await sut.put(new CacheKey('key.two', getSortKey(sortKey)), 2);

    const transactionKeys = await sut.keys(getSortKey(sortKey));
    expect(transactionKeys).toContain('key.one');
    expect(transactionKeys).toContain('key.two');

    const kvKeys = Array.from((await sut.kvMap(getSortKey(sortKey), { gte: 'key.', lt: 'key.\xff' })).keys());
    expect(kvKeys).toContain('key.one');
    expect(kvKeys).toContain('key.two');

    await sut.commit();

    expect((await sut.getLast('key.one')).cachedValue).toEqual(1);
    expect((await sut.getLast('key.three'))?.cachedValue).toBeFalsy();

    await sut.close();
  });

  it('keys order natural and reversed', async () => {
    await sut.open();
    const sortKey = 348;

    await sut.begin();

    await sut.put(new CacheKey('user.11', getSortKey(sortKey)), 2);
    await sut.put(new CacheKey('user.12', getSortKey(sortKey)), 2);
    await sut.put(new CacheKey('user.13', getSortKey(sortKey)), 2);
    await sut.put(new CacheKey('user.14', getSortKey(sortKey)), 2);
    await sut.put(new CacheKey('user.15', getSortKey(sortKey)), 2);

    const naturalOrder = Array.from((await sut.kvMap(getSortKey(sortKey), { gte: 'user.11', lt: 'user.14' })).keys());
    const reverseOrder = Array.from(
      (
        await sut.kvMap(getSortKey(sortKey), {
          gte: 'user.11',
          lt: 'user.14',
          reverse: true
        })
      ).keys()
    );
    expect(naturalOrder.reverse()).toEqual(reverseOrder);

    await sut.commit();

    await sut.begin();
    await sut.del(new CacheKey('user.12', getSortKey(sortKey)));

    const items = Array.from(
      (
        await sut.kvMap(getSortKey(sortKey), {
          gte: 'user.11',
          lt: 'user.14',
          reverse: true
        })
      ).keys()
    );
    expect(items).toEqual(['user.13', 'user.11']);

    await sut.commit();
    await sut.close();
  });

  it('access range keys during active transaction and rollback', async () => {
    await sut.open();
    const sortKey = 384;

    await sut.begin();
    await sut.put(new CacheKey('key.one', getSortKey(sortKey)), 11);
    await sut.put(new CacheKey('key.three', getSortKey(sortKey)), 3);
    await sut.del(new CacheKey('key.two', getSortKey(sortKey)));

    const transactionKeys = await sut.keys(getSortKey(sortKey));
    expect(transactionKeys).toContain('key.one');
    expect(transactionKeys).toContain('key.three');

    const kvKeys = Array.from((await sut.kvMap(getSortKey(sortKey), { gte: 'key.', lt: 'key.\xff' })).keys());
    expect(kvKeys).toContain('key.one');
    expect(kvKeys).toContain('key.three');

    expect((await sut.getLast('key.one')).cachedValue).toEqual(11);
    expect((await sut.getLast('key.two'))?.cachedValue).toBeFalsy();
    expect((await sut.getLast('key.three')).cachedValue).toEqual(3);

    await sut.rollback();

    expect((await sut.getLast('key.one')).cachedValue).toEqual(1);
    expect((await sut.getLast('key.two')).cachedValue).toEqual(2);
    expect((await sut.getLast('key.three'))?.cachedValue).toBeFalsy();

    await sut.close();
  });

  it('multiple operations', async () => {
    await sut.open();
    const sortKey = 395;

    await sut.begin();
    await sut.put(new CacheKey('key.one', getSortKey(sortKey)), 111);
    await sut.put(new CacheKey('key.two', getSortKey(sortKey)), 222);
    await sut.put(new CacheKey('key.four', getSortKey(sortKey)), 333);
    await sut.put(new CacheKey('key.five', getSortKey(sortKey)), 333);

    await sut.del(new CacheKey('key.two', getSortKey(sortKey)));
    await sut.del(new CacheKey('key.fa', getSortKey(sortKey)));

    const transactionKeys = await sut.keys(getSortKey(sortKey));
    expect(transactionKeys).toContain('key.one');
    expect(transactionKeys).toContain('key.four');

    const kvKeys = Array.from(
      (
        await sut.kvMap(getSortKey(sortKey), {
          gte: 'key.',
          lt: 'key.\xff',
          limit: 2
        })
      ).keys()
    );
    expect(kvKeys).toEqual(['key.five', 'key.four']);

    await sut.rollback();

    const rollbackKeys = Array.from(
      (
        await sut.kvMap(getSortKey(sortKey), {
          gte: 'key.',
          lt: 'key.\xff',
          reverse: true
        })
      ).keys()
    );
    expect(rollbackKeys).toEqual(['key.two', 'key.one']);

    await sut.drop();
    await sut.close();
  });
});
