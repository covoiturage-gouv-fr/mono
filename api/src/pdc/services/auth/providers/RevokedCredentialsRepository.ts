import { provider } from "@/ilos/common/index.ts";
import { RedisConnection } from "@/ilos/connection-redis/index.ts";

// Must outlive Dex `expiry.idTokens` (5 min, infra cluster/extensions/dex/*/config.yaml).
export const REVOCATION_TTL_SECONDS = 600;
const KEY_PREFIX = "auth:revoked:";

/**
 * Deleted access keys, kept until every token they issued has expired:
 * Dex tokens stay valid until `exp` once signed.
 */
@provider()
export class RevokedCredentialsRepository {
  constructor(private redis: RedisConnection) {}

  async revoke(tokenIds: string[]): Promise<void> {
    if (!tokenIds.length) return;
    const tx = this.redis.getClient().multi();
    for (const id of tokenIds) {
      tx.set(`${KEY_PREFIX}${id}`, "1", "EX", REVOCATION_TTL_SECONDS);
    }
    // exec() reports per-command failures in its result instead of rejecting
    const failed = (await tx.exec())?.find(([err]) => err);
    if (failed) throw failed[0];
  }

  async isRevoked(tokenId: string): Promise<boolean> {
    return (await this.redis.getClient().exists(`${KEY_PREFIX}${tokenId}`)) === 1;
  }
}
