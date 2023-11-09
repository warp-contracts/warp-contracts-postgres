import { ClientConfig } from 'pg';

export interface PgSortKeyCacheOptions extends ClientConfig {
  schemaName: string;
  tableName: string;
  minEntriesPerKey: number;
  maxEntriesPerKey: number;
}
