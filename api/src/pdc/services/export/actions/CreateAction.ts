import { ContextType, handler, InvalidParamsException } from "@/ilos/common/index.ts";
import { Action as AbstractAction } from "@/ilos/core/index.ts";
import { DefaultTimezoneMiddleware } from "@/pdc/middlewares/DefaultTimezoneMiddleware.ts";
import {
  castToArrayMiddleware,
  copyFromContextMiddleware,
  hasPermissionMiddleware,
  validateDateMiddleware,
} from "@/pdc/providers/middleware/middlewares.ts";
import { maxEndDefault, minStartDefault } from "../config/export.ts";
import { handlerConfig, ParamsInterface, ResultInterface } from "../contracts/create.contract.ts";
import { aliasV3 } from "../contracts/create.schema.ts";
import { Export } from "../models/Export.ts";
import { ExportParams } from "../models/ExportParams.ts";
import { ExportRecipient } from "../models/ExportRecipient.ts";
import { ExportRepositoryInterfaceResolver } from "../repositories/ExportRepository.ts";
import { RecipientServiceInterfaceResolver } from "../services/RecipientService.ts";
import { TerritoryServiceInterfaceResolver } from "../services/TerritoryService.ts";

@handler({
  ...handlerConfig,
  middlewares: [
    hasPermissionMiddleware("common.export.create"),
    ["timezone", DefaultTimezoneMiddleware],
    copyFromContextMiddleware(`call.user._id`, "created_by", true),
    copyFromContextMiddleware(`call.user.operator_id`, "operator_id", false),
    copyFromContextMiddleware(
      `call.user.territory_id`,
      "territory_id",
      undefined,
    ),
    castToArrayMiddleware(["operator_id", "territory_id", "recipients"]),
    validateDateMiddleware({
      startPath: "start_at",
      endPath: "end_at",
      minStart: () => new Date(new Date().getTime() - minStartDefault),
      maxEnd: () => new Date(new Date().getTime() - maxEndDefault),
    }),
    ["validate", aliasV3],
  ],
  apiRoute: {
    path: "/exports",
    method: "POST",
    successHttpCode: 201,
  },
})
export class CreateActionV3 extends AbstractAction {
  constructor(
    protected exportRepository: ExportRepositoryInterfaceResolver,
    protected territoryService: TerritoryServiceInterfaceResolver,
    protected recipientService: RecipientServiceInterfaceResolver,
  ) {
    super();
  }

  protected override async handle(
    params: ParamsInterface,
    context: ContextType,
  ): Promise<ResultInterface> {
    const paramTarget = Export.target(context);

    // make sure we have at least one recipient
    const recipients = await this.recipientService.maybeAddCreator(
      (params.recipients || []).map(ExportRecipient.fromEmail),
      params.created_by,
    );

    if (!recipients.length) {
      throw new InvalidParamsException(
        'No recipient found! You must set "created_by" or "recipients"',
      );
    }

    // Create the export request
    const {
      uuid,
      target,
      status,
      params: createParams,
    } = await this.exportRepository.create({
      created_by: params.created_by,
      target: paramTarget,
      recipients: recipients,
      params: new ExportParams({
        tz: params.tz,
        start_at: params.start_at,
        end_at: params.end_at,
        operator_id: params.operator_id,
        geo_selector: await this.territoryService.resolve({
          territory_id: params.territory_id,
          geo_selector: params.geo_selector,
        }),
      }),
    });

    return {
      uuid,
      target,
      status,
      start_at: new Date(createParams.get().start_at),
      end_at: new Date(createParams.get().end_at),
    };
  }
}
