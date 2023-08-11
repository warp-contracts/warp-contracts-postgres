import { ClientConfig } from "pg";

export interface PgCacheOptions extends ClientConfig {
  maxEntriesPerContract: number;
}
