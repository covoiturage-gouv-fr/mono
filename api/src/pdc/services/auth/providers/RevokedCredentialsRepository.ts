import { provider } from "@/ilos/common/index.ts";
import { RedisConnection } from "@/ilos/connection-redis/index.ts";

// Must outlive Dex `expiry.idTokens` (5 min, infra cluster/extensions/dex/*/config.yaml).
// DexOIDCProvider refuses older tokens, so the mark always outlives them.
export const REVOCATION_TTL_SECONDS = 600;
const KEY_PREFIX = "auth:revoked:";

// Enough to find the key in Dex without logging it whole
export const keyRef = (tokenId: string): string => tokenId.slice(0, 8);

type Transaction = ReturnType<ReturnType<RedisConnection["getClient"]>["multi"]>;

/**
 * Deleted access keys, kept until every token they issued has expired:
 * Dex tokens stay valid until `exp` once signed.
 */
@provider()
export class RevokedCredentialsRepository {
  constructor(private redis: RedisConnection) {}

  // No TTL until the key is gone from Dex: a failed delete must not expire the mark
  async revoke(tokenIds: string[]): Promise<void> {
    await this.exec(tokenIds, (tx, key) => tx.set(key, "1"));
  }

  async expire(tokenIds: string[]): Promise<void> {
    await this.exec(tokenIds, (tx, key) => tx.expire(key, REVOCATION_TTL_SECONDS));
  }

  async isRevoked(tokenId: string): Promise<boolean> {
    return (await this.redis.getClient().exists(`${KEY_PREFIX}${tokenId}`)) === 1;
  }

  protected async exec(
    tokenIds: string[],
    command: (tx: Transaction, key: string) => void,
  ): Promise<void> {
    if (!tokenIds.length) return;
    const tx = this.redis.getClient().multi();
    for (const id of tokenIds) command(tx, `${KEY_PREFIX}${id}`);
    // exec() reports per-command failures in its result instead of rejecting
    const failed = (await tx.exec())?.find(([err]) => err);
    if (failed) throw failed[0];
  }
}
