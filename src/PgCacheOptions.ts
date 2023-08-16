import { ClientConfig } from "pg";

export interface PgCacheOptions extends ClientConfig {
  minEntriesPerContract: number;
  maxEntriesPerContract: number;
}
