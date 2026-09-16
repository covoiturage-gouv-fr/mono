import { ContextType, ForbiddenException, handler } from "@/ilos/common/index.ts";
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
import { ExportRepositoryInterfaceResolver } from "../repositories/ExportRepository.ts";
import { TerritoryServiceInterfaceResolver } from "../services/TerritoryService.ts";

@handler({
  ...handlerConfig,
  middlewares: [
    hasPermissionMiddleware("common.export.create"),
    ["timezone", DefaultTimezoneMiddleware],
    // preserve=false : l'auteur vient de la session. Sinon le corps le fixe librement et
    // l'export part au nom d'un tiers, qui en reçoit la notification et le lien de téléchargement.
    copyFromContextMiddleware(`call.user._id`, "created_by", false),
    copyFromContextMiddleware(`call.user.operator_id`, "operator_id", false),
    copyFromContextMiddleware(
      `call.user.territory_id`,
      "territory_id",
      undefined,
    ),
    castToArrayMiddleware(["operator_id", "territory_id"]),
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
export class CreateAction extends AbstractAction {
  constructor(
    protected exportRepository: ExportRepositoryInterfaceResolver,
    protected territoryService: TerritoryServiceInterfaceResolver,
  ) {
    super();
  }

  /**
   * Périmètre géographique de l'export, borné à celui de l'appelant.
   *
   * `resolve()` donne priorité au `geo_selector` sur `territory_id` : accepté depuis le corps,
   * il permet à une session de territoire d'exporter n'importe quel périmètre, France entière
   * comprise, avec des données de grade territoire (clés d'identité, coordonnées, opérateur).
   * Un appelant territoire n'a donc que son `territory_id`, résolu côté serveur.
   */
  private scopedGeo(params: ParamsInterface, context: ContextType) {
    const ownTerritory = context?.call?.user?.territory_id;
    return {
      territory_id: params.territory_id,
      geo_selector: ownTerritory ? undefined : params.geo_selector,
    };
  }

  protected override async handle(
    params: ParamsInterface,
    context: ContextType,
  ): Promise<ResultInterface> {
    const paramTarget = Export.target(context);

    // Sans auteur, l'INSERT viole la contrainte NOT NULL et rend un 500 : on refuse proprement.
    if (typeof params.created_by !== "number") {
      throw new ForbiddenException("Export creation requires a user session");
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
      params: new ExportParams({
        tz: params.tz,
        start_at: params.start_at,
        end_at: params.end_at,
        operator_id: params.operator_id,
        geo_selector: await this.territoryService.resolve(this.scopedGeo(params, context)),
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
