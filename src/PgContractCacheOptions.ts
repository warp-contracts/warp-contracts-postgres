import { ClientConfig } from 'pg';

export interface PgContractCacheOptions extends ClientConfig {
  minEntriesPerContract: number;
  maxEntriesPerContract: number;
}
