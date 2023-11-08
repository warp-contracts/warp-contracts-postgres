import { PgContractCache } from "../PgContractCache";
import { PgContractCacheOptions } from "../PgContractCacheOptions";
import { EvalStateResult } from "warp-contracts";
import { PgSortKeyCacheOptions } from "../PgSortKeyCacheOptions";
import { PgSortKeyCache } from "../PgSortKeyCache";

export const getContractId = (i: number) => `contract${i}`.padStart(43, "0");

export const getSortKey = (j: number) =>
  `${j
    .toString()
    .padStart(
      12,
      "0"
    )},1643210931796,81e1bea09d3262ee36ce8cfdbbb2ce3feb18a717c3020c47d206cb8ecb43b767`;

export const contractCache = async function (
  numContracts: number,
  numRepeatingEntries: number,
  maxEntries?: number
): Promise<PgContractCache<any>> {
  const pgOptions: PgContractCacheOptions = {
    minEntriesPerContract: maxEntries || 100 * numRepeatingEntries,
    maxEntriesPerContract: maxEntries || 100 * numRepeatingEntries,
    user: 'postgres',
    password: 'postgres',
    host: 'localhost',
    database: 'postgres',
    port: 5432
  };

  const sut = new PgContractCache<unknown>(pgOptions);
  await sut.open();

  for (let i = 0; i < numContracts; i++) {
    for (let j = 0; j < numRepeatingEntries; j++) {
      await sut.put(
        {
          key: getContractId(i),
          sortKey: getSortKey(j),
        },
        evalState(`{ "contract${i}":"${j}" }`)
      );
    }
  }

  return sut;
};

export const sortKeyCache = async function (
  numRepeatingEntries: number,
  maxEntries?: number
): Promise<PgSortKeyCache<any>> {
  const pgOptions: PgSortKeyCacheOptions = {
    tableName: "kiwi",
    minEntriesPerKey: maxEntries || 100 * numRepeatingEntries,
    maxEntriesPerKey: maxEntries || 100 * numRepeatingEntries,
    user: 'postgres',
    password: 'postgres',
    host: 'localhost',
    database: 'postgres',
    port: 5432
  };

  return new PgSortKeyCache<unknown>(pgOptions);
};

export const evalState = function (value: any) {
  return new EvalStateResult(value, {}, {});
};
