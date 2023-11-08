import { ClientConfig } from 'pg';

export interface PgSortKeyCacheOptions extends ClientConfig {
  tableName: string;
  minEntriesPerKey: number;
  maxEntriesPerKey: number;
}
