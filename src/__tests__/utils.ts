import { PgContractCache } from "../PgContractCache";
import { PgCacheOptions } from "../PgCacheOptions";
import { defaultCacheOptions, EvalStateResult } from "warp-contracts";

export const getContractId = (i: number) => `contract${i}`.padStart(43, "0");

export const getSortKey = (j: number) =>
  `${j
    .toString()
    .padStart(
      12,
      "0"
    )},1643210931796,81e1bea09d3262ee36ce8cfdbbb2ce3feb18a717c3020c47d206cb8ecb43b767`;

export const cache = async function (
  numContracts: number,
  numRepeatingEntries: number,
  maxEntries?: number
): Promise<PgContractCache<any>> {
  const pgOptions: PgCacheOptions = {
    minEntriesPerContract: maxEntries || 100 * numRepeatingEntries,
    maxEntriesPerContract: maxEntries || 100 * numRepeatingEntries,
    user: "postgres",
    database: "postgres",
    port: 5432,
  };

  const sut = new PgContractCache<unknown>(defaultCacheOptions, pgOptions);
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

export const evalState = function (value: any) {
  return new EvalStateResult(value, {}, {});
};
