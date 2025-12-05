import { handler } from "@/ilos/common/Decorators.ts";
import { Action as AbstractAction } from "@/ilos/core/index.ts";
import { UnauthorizedException, UnexpectedException } from "../../../../ilos/common/index.ts";
import { CreateAccessTokenParams, CreateAccessTokenResult } from "../dto/Credentials.ts";
import { DexClient } from "../providers/DexClient.ts";
import { DexOIDCProvider } from "../providers/DexOIDCProvider.ts";

@handler({
  service: "auth",
  method: "accessToken",
  middlewares: [
    ["validate", CreateAccessTokenParams],
  ],
  apiRoute: {
    path: "/auth/access_token",
    method: "POST",
    public: true,
    rateLimiter: {
      key: "rl-auth",
      limit: 20,
      windowMinute: 1,
    },
    successHttpCode: 201,
    rpcAnswerOnFailure: true,
  },
})
export class CreateAccessTokenAction extends AbstractAction {
  constructor(private dexClient: DexClient, private dexOIDCProvider: DexOIDCProvider) {
    super();
  }

  protected override async handle(params: CreateAccessTokenParams): Promise<CreateAccessTokenResult> {
    try {
      const { access_key, secret_key } = params;
      const access_token = await this.dexOIDCProvider.getToken(access_key, secret_key);

      return { access_token };
    } catch (e) {
      if (Error.isError(e)) {
        if (e.message === "Unauthorized") {
          throw new UnauthorizedException("Invalid access key or secret key");
        }

        throw new UnexpectedException("Failed to create access token");
      }

      throw e;
    }
  }
}
