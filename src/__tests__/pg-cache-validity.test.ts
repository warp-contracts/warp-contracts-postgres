import { contractCache, evalState } from './utils';
import { CacheKey } from 'warp-contracts';

describe('Postgres cache', () => {
  it('should return proper validity', async () => {
    const sut = await contractCache(0, 100);

    await sut.put(
      {
        key: 'contract0',
        sortKey: '000000860512,1643210931796,81e1bea09d3262ee36ce8cfdbbb2ce3feb18a717c3020c47d206cb8ecb43b767'
      },
      evalState(
        { result: 'contract0:sortKey0' },
        {
          zz11: true,
          zz12: false,
          zz13: true,
          zz14: true,
          zz15: true
        },
        {
          zz12: 'bum!!'
        }
      )
    );
    await sut.put(
      {
        key: 'contract1',
        sortKey: '000000860513,1643210931796,81e1bea09d3262ee36ce8cfdbbb2ce3feb18a717c3020c47d206cb8ecb43b767'
      },
      evalState(
        { result: 'contract1:sortKey1' },
        {
          zz11: true,
          zz12: false,
          zz13: true
        },
        {
          zz12: 'bum!!'
        }
      )
    );
    await sut.put(
      {
        key: 'contract0',
        sortKey: '000000860514,1643210931796,81e1bea09d3262ee36ce8cfdbbb2ce3feb18a717c3020c47d206cb8ecb43b767'
      },
      evalState(
        { result: 'contract1:sortKey2' },
        {
          zz16: true,
          zz17: false,
          zz18: true
        },
        {
          zz17: 'bum!!'
        }
      )
    );
    await sut.setSignature(
      {
        key: 'contract0',
        sortKey: '000000860514,1643210931796,81e1bea09d3262ee36ce8cfdbbb2ce3feb18a717c3020c47d206cb8ecb43b767'
      },
      'asd',
      'asd'
    );

    expect(
      await sut.get(
        new CacheKey(
          'contract0',
          '000000860514,1643210931796,81e1bea09d3262ee36ce8cfdbbb2ce3feb18a717c3020c47d206cb8ecb43b767'
        )
      )
    ).toEqual({
      sortKey: '000000860514,1643210931796,81e1bea09d3262ee36ce8cfdbbb2ce3feb18a717c3020c47d206cb8ecb43b767',
      cachedValue: evalState({ result: 'contract1:sortKey2' })
    });

    expect(
      await sut.getValidityAll(
        new CacheKey(
          'contract0',
          '000000860514,1643210931796,81e1bea09d3262ee36ce8cfdbbb2ce3feb18a717c3020c47d206cb8ecb43b767'
        )
      )
    ).toEqual({
      count: '8',
      validity: {
        zz11: true,
        zz12: false,
        zz13: true,
        zz14: true,
        zz15: true,
        zz16: true,
        zz17: false,
        zz18: true
      }
    });

    await sut.drop();
    await sut.close();
  });
});
