import { ConfigInterfaceResolver, provider } from "@/ilos/common/index.ts";
import { logger } from "@/lib/logger/index.ts";
import { SESSION_KEY_PREFIX } from "@/pdc/proxy/middlewares/sessionMiddleware.ts";
import { Redis } from "dep:redis";

/**
 * Purge des sessions Redis d'un utilisateur.
 *
 * connect-redis n'indexe pas les sessions par user : elles sont stockées sous
 * `${SESSION_KEY_PREFIX}<sid>` avec une valeur JSON `{ cookie, user }`. Faute d'index,
 * on scanne le préfixe et on filtre sur `user._id` (approche pragmatique, cf. plan T4).
 * Volume attendu faible (sessions d'agents), SCAN non bloquant par lots.
 */
@provider()
export class SessionRepository {
  private client: Redis | null = null;

  constructor(private config: ConfigInterfaceResolver) {}

  protected getClient(): Redis {
    if (!this.client) {
      this.client = new Redis(this.config.get("connections.redis"));
    }
    return this.client;
  }

  // Détruit toutes les sessions Redis appartenant à l'utilisateur donné.
  async destroyByUser(userId: number): Promise<void> {
    const client = this.getClient();
    const pattern = `${SESSION_KEY_PREFIX}*`;
    let cursor = 0;

    do {
      const [cur, keys] = await client.scan(cursor, "MATCH", pattern, "COUNT", 100);
      cursor = parseInt(cur, 10);

      for (const key of keys) {
        const raw = await client.get(key);
        if (!raw) continue;
        try {
          const session = JSON.parse(raw);
          if (session?.user?._id === userId) {
            await client.del(key);
          }
        } catch (_e) {
          // Session non-JSON ou corrompue : on l'ignore (ne bloque pas la purge).
          logger.warn(`[session] clé non parsable ignorée: ${key}`);
        }
      }
    } while (cursor > 0);
  }
}
