import { handler } from "@/ilos/common/index.ts";
import { Action as AbstractAction } from "@/ilos/core/index.ts";
import { AiresCovoiturage } from "@/pdc/services/observatory/dto/infra/AiresCovoiturage.ts";
import type { Feature } from "dep:turf-helpers";
import { InfraRepositoryInterfaceResolver } from "../../interfaces/InfraRepositoryProviderInterface.ts";
export type ResultInterface = {
  id_lieu: string;
  nom_lieu: string;
  com_lieu: string;
  type: string;
  date_maj: Date;
  nbre_pl: number;
  nbre_pmr: number;
  duree: number;
  horaires: string;
  proprio: string;
  lumiere: boolean;
  geom: Feature;
}[];

@handler({
  service: "observatory",
  method: "airesCovoiturage",
  middlewares: [[
    "validate",
    AiresCovoiturage,
  ]],
  apiRoute: {
    path: "/observatory/aires-covoiturage",
    method: "GET",
    public: true,
  },
})
export class AiresCovoiturageAction extends AbstractAction {
  constructor(private repository: InfraRepositoryInterfaceResolver) {
    super();
  }

  public override async handle(params: AiresCovoiturage): Promise<ResultInterface> {
    return this.repository.getAiresCovoiturage(params);
  }
}
