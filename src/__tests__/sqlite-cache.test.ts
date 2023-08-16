import { cache, getContractId, getSortKey, evalState } from "./utils";
import { CacheKey } from "warp-contracts";

describe("Sqlite cache", () => {
  it("should return proper data", async () => {
    const sut = await cache(0, 100);

    await sut.put(
      {
        key: "contract0",
        sortKey:
          "000000860512,1643210931796,81e1bea09d3262ee36ce8cfdbbb2ce3feb18a717c3020c47d206cb8ecb43b767",
      },
      evalState({ result: "contract0:sortKey0" })
    );
    await sut.put(
      {
        key: "contract1",
        sortKey:
          "000000860513,1643210931796,81e1bea09d3262ee36ce8cfdbbb2ce3feb18a717c3020c47d206cb8ecb43b767",
      },
      evalState({ result: "contract1:sortKey1" })
    );
    await sut.put(
      {
        key: "contract1",
        sortKey:
          "000000860514,1643210931796,81e1bea09d3262ee36ce8cfdbbb2ce3feb18a717c3020c47d206cb8ecb43b767",
      },
      evalState({ result: "contract1:sortKey2" })
    );
    await sut.put(
      {
        key: "contract1",
        sortKey:
          "000000860515,1643210931796,81e1bea09d3262ee36ce8cfdbbb2ce3feb18a717c3020c47d206cb8ecb43b767",
      },
      evalState({ result: "contract1:sortKey3" })
    );
    await sut.put(
      {
        key: "contract2",
        sortKey:
          "000000860513,1643210931888,81e1bea09d3262ee36ce8cfdbbb2ce3feb18a717c3020c47d206cb8ecb43b767",
      },
      evalState({ result: "contract2:sortKey1" })
    );

    expect(
      await sut.get(
        new CacheKey(
          "contract2",
          "000000860513,1643210931888,81e1bea09d3262ee36ce8cfdbbb2ce3feb18a717c3020c47d206cb8ecb43b767"
        )
      )
    ).toEqual({
      sortKey:
        "000000860513,1643210931888,81e1bea09d3262ee36ce8cfdbbb2ce3feb18a717c3020c47d206cb8ecb43b767",
      cachedValue: evalState({ result: "contract2:sortKey1" }),
    });
    expect(
      await sut.get(
        new CacheKey(
          "contract1",
          "000000860514,1643210931796,81e1bea09d3262ee36ce8cfdbbb2ce3feb18a717c3020c47d206cb8ecb43b767"
        )
      )
    ).toEqual({
      sortKey:
        "000000860514,1643210931796,81e1bea09d3262ee36ce8cfdbbb2ce3feb18a717c3020c47d206cb8ecb43b767",
      cachedValue: evalState({ result: "contract1:sortKey2" }),
    });
    expect(
      await sut.get(
        new CacheKey(
          "contract0",
          "000000860512,1643210931796,81e1bea09d3262ee36ce8cfdbbb2ce3feb18a717c3020c47d206cb8ecb43b767"
        )
      )
    ).toEqual({
      sortKey:
        "000000860512,1643210931796,81e1bea09d3262ee36ce8cfdbbb2ce3feb18a717c3020c47d206cb8ecb43b767",
      cachedValue: evalState({ result: "contract0:sortKey0" }),
    });
    expect(
      await sut.get(
        new CacheKey(
          "contract0",
          "000000860512,1643210931796,81e1bea09d3262ee36ce8cfdbbb2ce3feb18a717c3020c47d206cb8ecb43b766"
        )
      )
    ).toEqual(null);
    expect(
      await sut.get(
        new CacheKey(
          "contract2",
          "000000860514,1643210931888,81e1bea09d3262ee36ce8cfdbbb2ce3feb18a717c3020c47d206cb8ecb43b767"
        )
      )
    ).toEqual(null);
    expect(
      await sut.get(
        new CacheKey(
          "contract1",
          "000000860516,1643210931796,81e1bea09d3262ee36ce8cfdbbb2ce3feb18a717c3020c47d206cb8ecb43b767"
        )
      )
    ).toEqual(null);

    expect(
      await sut.getLessOrEqual(
        "contract2",
        "000000860513,1643210931888,81e1bea09d3262ee36ce8cfdbbb2ce3feb18a717c3020c47d206cb8ecb43b767"
      )
    ).toEqual({
      sortKey:
        "000000860513,1643210931888,81e1bea09d3262ee36ce8cfdbbb2ce3feb18a717c3020c47d206cb8ecb43b767",
      cachedValue: evalState({ result: "contract2:sortKey1" }),
    });
    expect(
      await sut.getLessOrEqual(
        "contract2",
        "000000860513,1643210931888,81e1bea09d3262ee36ce8cfdbbb2ce3feb18a717c3020c47d206cb8ecb43b768"
      )
    ).toEqual({
      sortKey:
        "000000860513,1643210931888,81e1bea09d3262ee36ce8cfdbbb2ce3feb18a717c3020c47d206cb8ecb43b767",
      cachedValue: evalState({ result: "contract2:sortKey1" }),
    });
    expect(
      await sut.getLessOrEqual(
        "contract2",
        "000000860513,1643210931888,81e1bea09d3262ee36ce8cfdbbb2ce3feb18a717c3020c47d206cb8ecb43b766"
      )
    ).toEqual(null);

    expect(
      await sut.getLessOrEqual(
        "contract1",
        "000000860513,1643210931796,81e1bea09d3262ee36ce8cfdbbb2ce3feb18a717c3020c47d206cb8ecb43b767"
      )
    ).toEqual({
      sortKey:
        "000000860513,1643210931796,81e1bea09d3262ee36ce8cfdbbb2ce3feb18a717c3020c47d206cb8ecb43b767",
      cachedValue: evalState({ result: "contract1:sortKey1" }),
    });

    expect(
      await sut.getLessOrEqual(
        "contract1",
        "000000860513,1643210931796,81e1bea09d3262ee36ce8cfdbbb2ce3feb18a717c3020c47d206cb8ecb43b766"
      )
    ).toEqual(null);
    expect(
      await sut.getLessOrEqual(
        "contract0",
        "000000860512,1643210931796,81e1bea09d3262ee36ce8cfdbbb2ce3feb18a717c3020c47d206cb8ecb43b768"
      )
    ).toEqual({
      sortKey:
        "000000860512,1643210931796,81e1bea09d3262ee36ce8cfdbbb2ce3feb18a717c3020c47d206cb8ecb43b767",
      cachedValue: evalState({ result: "contract0:sortKey0" }),
    });
    expect(
      await sut.getLessOrEqual(
        "contract0",
        "000000860512,1643210931796,81e1bea09d3262ee36ce8cfdbbb2ce3feb18a717c3020c47d206cb8ecb43b767"
      )
    ).toEqual({
      sortKey:
        "000000860512,1643210931796,81e1bea09d3262ee36ce8cfdbbb2ce3feb18a717c3020c47d206cb8ecb43b767",
      cachedValue: evalState({ result: "contract0:sortKey0" }),
    });
    expect(
      await sut.getLessOrEqual(
        "contract0",
        "000000860512,1643210931796,81e1bea09d3262ee36ce8cfdbbb2ce3feb18a717c3020c47d206cb8ecb43b765"
      )
    ).toEqual(null);

    await sut.drop();
    await sut.close();
  });

  it("respects limits for max interactions per contract", async () => {
    const max = 10;
    const sut = await cache(0, 0, max);

    for (let j = 0; j < max; j++) {
      await sut.put(
        {
          key: getContractId(0),
          sortKey: getSortKey(j),
        },
        evalState({ result: `contract${0}:${j}` })
      );
    }

    // All entries are available
    for (let j = 0; j < max; ++j) {
      const result = await sut.get(
        new CacheKey(getContractId(0), getSortKey(j))
      );
      expect(result).toBeTruthy();
      expect(result?.cachedValue.state.result).toBe(`contract${0}:${j}`);
    }

    // This put causes cleanup
    await sut.put(
      {
        key: getContractId(0),
        sortKey: getSortKey(max),
      },
      evalState({ result: `contract${0}:${max}` })
    );

    for (let i = 0; i <= max; i++) {
      const result = await sut.get(
        new CacheKey(getContractId(0), getSortKey(i))
      );
      if (i == 0) {
        expect(result).toBeFalsy();
      } else {
        expect(result).toBeTruthy();
        expect(result?.cachedValue.state.result).toBe(`contract${0}:${i}`);
      }
    }

    // This just adds another entry, and cleanup
    await sut.put(
      {
        key: getContractId(0),
        sortKey: getSortKey(max + 1),
      },
      evalState({ result: `contract${0}:${max + 1}` })
    );

    for (let i = 0; i <= max + 1; i++) {
      const result = await sut.get(
        new CacheKey(getContractId(0), getSortKey(i))
      );
      if (i <= 1) {
        expect(result).toBeFalsy();
      } else {
        expect(result).toBeTruthy();
        expect(result!.cachedValue.state.result).toBe(`contract${0}:${i}`);
      }
    }
    await sut.drop();
    await sut.close();
  });

  it("should keep the latest insert, even it is the smallest one", async () => {
    const sut = await cache(0, 0, 2);

    await sut.put(
      {
        key: getContractId(0),
        sortKey: getSortKey(5),
      },
      evalState({ result: `contract5` })
    );
    await sut.put(
      {
        key: getContractId(0),
        sortKey: getSortKey(7),
      },
      evalState({ result: `contract7` })
    );
    await sut.put(
      {
        key: getContractId(0),
        sortKey: getSortKey(2),
      },
      evalState({ result: `contract2` })
    );

    const result5 = await sut.get(
      new CacheKey(getContractId(0), getSortKey(5))
    );
    const result7 = await sut.get(
      new CacheKey(getContractId(0), getSortKey(7))
    );
    const result2 = await sut.get(
      new CacheKey(getContractId(0), getSortKey(2))
    );

    expect(result5).toBeFalsy();
    expect(result7).toBeTruthy();
    expect(result2).toBeTruthy();

    await sut.drop();
    await sut.close();
  });
});
