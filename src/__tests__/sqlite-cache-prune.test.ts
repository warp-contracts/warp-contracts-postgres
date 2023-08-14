import { cache, getContractId, getSortKey } from "./utils";
import { CacheKey } from "warp-contracts";

describe("Sqlite cache prune", () => {
  it("handle improper args", async () => {
    const contracts = 10;
    const entriesPerContract = 1;
    const sut = await cache(contracts, entriesPerContract);

    const noopStats = { entriesAfter: contracts, entriesBefore: contracts };
    expect(await sut.prune(0)).toMatchObject(noopStats);
    expect(await sut.prune(-1)).toMatchObject(noopStats);
    await sut.drop();
    await sut.close();
  });

  it("no deletion should be performed", async () => {
    const contracts = 10;
    const entriesPerContract = 1;
    const sut = await cache(contracts, entriesPerContract);

    const noopStats = { entriesAfter: contracts, entriesBefore: contracts };
    expect(await sut.prune(1)).toMatchObject(noopStats);
    expect(await sut.prune(10)).toMatchObject(noopStats);
    expect(await sut.prune(contracts)).toMatchObject(noopStats);
    expect(await sut.prune(-1 * contracts)).toMatchObject(noopStats);
    expect(await sut.prune(contracts)).toMatchObject(noopStats);
    expect(await sut.prune(2 * contracts)).toMatchObject(noopStats);
    await sut.drop();
    await sut.close();
  });

  it("should remove all unneeded entries, one contract", async () => {
    const contracts = 1;
    const entriesPerContract = 10;
    const sut = await cache(contracts, entriesPerContract);
    expect(await sut.prune(1)).toMatchObject({
      entriesBefore: contracts * entriesPerContract,
      entriesAfter: contracts * 1,
    });
    await sut.drop();
    await sut.close();
  });

  it("should remove all unneeded entries, in many contracts", async () => {
    const contracts = 200;
    const entriesPerContract = 10;
    const sut = await cache(contracts, entriesPerContract);
    expect(await sut.prune(2)).toMatchObject({
      entriesBefore: contracts * entriesPerContract,
      entriesAfter: contracts * 2,
    });
    await sut.drop();
    await sut.close();
  });

  it("should remove oldest entries, in many contracts", async () => {
    const contracts = 100;
    const entriesPerContract = 20;
    const toLeave = 3;
    const sut = await cache(contracts, entriesPerContract);
    await sut.prune(toLeave);

    for (let i = 0; i < contracts; i++) {
      // Check newest elements are present
      for (let j = 0; j < toLeave; j++) {
        expect(
          await sut.get(
            new CacheKey(
              getContractId(i),
              getSortKey(entriesPerContract - j - 1)
            )
          )
        ).toBeTruthy();
      }

      // Check old elements are removed
      for (let j = toLeave; j < entriesPerContract; j++) {
        expect(
          await sut.get(
            new CacheKey(
              getContractId(i),
              getSortKey(entriesPerContract - j - 1)
            )
          )
        ).toBeFalsy();
      }
    }
    await sut.drop();
    await sut.close();
  });

  it("deletes first contract from cache", async () => {
    const contracts = 7;
    const entriesPerContract = 12;
    const sut = await cache(contracts, entriesPerContract);

    await sut.delete(getContractId(0));

    // Removed elements
    for (let j = 0; j < entriesPerContract; j++) {
      expect(
        await sut.get(new CacheKey(getContractId(0), getSortKey(j)))
      ).toBeFalsy();
    }

    // Remaining elements
    for (let i = 1; i < contracts; i++) {
      for (let j = 0; j < entriesPerContract; j++) {
        expect(
          await sut.get(new CacheKey(getContractId(i), getSortKey(j)))
        ).toBeTruthy();
      }
    }
    await sut.drop();
    await sut.close();
  });

  it("deletes contract from the middle of the cache", async () => {
    const contracts = 7;
    const entriesPerContract = 12;
    const removedContractIdx = 3;
    const sut = await cache(contracts, entriesPerContract);

    await sut.delete(getContractId(removedContractIdx));

    // Remaining elements
    for (let i = 0; i < contracts; i++) {
      for (let j = 0; j < entriesPerContract; j++) {
        const data = await sut.get(
          new CacheKey(getContractId(i), getSortKey(j))
        );
        if (i === removedContractIdx) {
          expect(data).toBeFalsy();
        } else {
          expect(data).toBeTruthy();
        }
      }
    }
    await sut.drop();
    await sut.close();
  });
});
