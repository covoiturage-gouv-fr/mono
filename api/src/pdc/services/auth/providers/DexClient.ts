import { ConfigInterfaceResolver, InitHookInterface, NotFoundException, provider } from "@/ilos/common/index.ts";
import { bcrypt_hash } from "@/lib/crypto/index.ts";
import { randomString, v4 } from "@/lib/uuid/index.ts";
import { Dex } from "@/pdc/services/auth/dex/generated/api.ts";
import { getDexClient } from "@/pdc/services/auth/dex/getDexClient.ts";
import { GrpcClient } from "dep:grpc";
import {
  CredentialsRole,
  DexClientCreateResult,
  DexClientDeleteResult,
  DexClientReadResult,
} from "../dto/Credentials.ts";
import { RevokedCredentialsRepository } from "./RevokedCredentialsRepository.ts";

@provider()
export class DexClient implements InitHookInterface {
  #client: (GrpcClient & Dex) | undefined;

  constructor(
    protected config: ConfigInterfaceResolver,
    protected revoked: RevokedCredentialsRepository,
  ) {}

  async init(): Promise<void> {
    await this.getClient();
  }

  protected async getClient() {
    if (this.#client) {
      return this.#client;
    }

    const hostname = this.config.get("dex.grpc_host");
    const port = this.config.get("dex.grpc_port");
    this.#client = await getDexClient({ hostname, port });

    return this.#client;
  }

  async readByOperator(id: number): Promise<DexClientReadResult> {
    const result = await (await this.getClient()).ListPasswords({});
    if (!result.passwords) {
      return [];
    }

    const filtered = result.passwords.map((p) => {
      const [role, operator_id] = p.username?.split(":") || [];
      return {
        token_id: p.email,
        operator_id: parseInt(operator_id),
        role: role as CredentialsRole,
      };
    }).filter((p) => p.operator_id === id);

    return filtered;
  }

  async createForOperator(operator_id: number, role = "operator.application"): Promise<DexClientCreateResult> {
    const access_key = v4();
    const secret_key = randomString();
    const hash = new TextEncoder().encode(await bcrypt_hash(secret_key));

    await (await this.getClient()).CreatePassword({
      password: {
        email: access_key,
        username: `${role}:${operator_id}`,
        hash,
        userId: access_key,
      },
    });

    return { access_key, secret_key };
  }

  async deleteByOperator(operator_id: number, token_id: string): Promise<DexClientDeleteResult> {
    const tokens = await this.readByOperator(operator_id);

    const exists = tokens.find((t) => t.operator_id === operator_id && t.token_id === token_id);
    if (!exists) {
      throw new NotFoundException(`[DexClient] Token with id ${token_id} not found for operator ${operator_id}`);
    }

    await this.revokeAndDelete([token_id]);
  }

  async deleteAllByOperator(operator_id: number): Promise<void> {
    const tokens = await this.readByOperator(operator_id);
    await this.revokeAndDelete(tokens.flatMap((t) => t.token_id ? [t.token_id] : []));
  }

  // Revoke first: if Dex fails, issued tokens are already refused
  protected async revokeAndDelete(tokenIds: string[]): Promise<void> {
    await this.revoked.revoke(tokenIds);
    const client = await this.getClient();
    for (const email of tokenIds) {
      await client.DeletePassword({ email });
    }
  }
}
