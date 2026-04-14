import { handler } from "@/ilos/common/index.ts";
import { Action as AbstractAction } from "@/ilos/core/index.ts";
import { IncentiveCampaigns } from "@/pdc/services/observatory/dto/IncentiveCampaigns.ts";
import type { Feature } from "dep:turf-helpers";
import { IncentiveCampaignsRepositoryInterfaceResolver } from "../../interfaces/IncentiveCampaignsRepositoryProviderInterface.ts";
export type ResultInterface = {
  type: string;
  code: string;
  premiere_campagne: string;
  budget_incitations: string;
  date_debut: string;
  date_fin: string;
  conducteur_montant_max_par_passager: string;
  conducteur_montant_max_par_mois: string;
  conducteur_montant_min_par_passager: string;
  conducteur_trajets_max_par_mois: string;
  passager_trajets_max_par_mois: string;
  passager_gratuite: string;
  passager_eligible_gratuite: string;
  passager_reduction_ticket: string;
  passager_eligibilite_reduction: string;
  passager_montant_ticket: string;
  zone_sens_des_trajets: string;
  zone_exclusion: string;
  si_zone_exclue_liste: string;
  autre_exclusion: string;
  trajet_longueur_min: string;
  trajet_longueur_max: string;
  trajet_classe_de_preuve: string;
  operateurs: string;
  autres_informations: string;
  lien: string;
  geom: Feature;
}[];

@handler({
  service: "observatory",
  method: "campaigns",
  middlewares: [[
    "validate",
    IncentiveCampaigns,
  ]],
  apiRoute: {
    path: "/observatory/campaigns",
    method: "GET",
    public: true,
  },
})
export class CampaignsAction extends AbstractAction {
  constructor(
    private repository: IncentiveCampaignsRepositoryInterfaceResolver,
  ) {
    super();
  }

  public override async handle(params: IncentiveCampaigns): Promise<ResultInterface> {
    return this.repository.getCampaigns(params);
  }
}
