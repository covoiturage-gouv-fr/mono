export type CarpoolStatus = "ok" | "fraud_error" | "anomaly_error" | "expired" | "canceled";

export interface SyntheticOperator {
  key: string;
  name: string;
  legal_name: string;
  siret: string;
  // Les campagnes filtrent les opérateurs sur cet UUID (OperatorsEnum du moteur de politiques)
  uuid: string;
}

export interface SyntheticCampaign {
  name: string;
  // identifiant d'un handler de services/policy/engine/policies
  handler: string;
  start_date: string;
  end_date: string;
  max_amount: number;
}

export interface SyntheticTerritory {
  key: string;
  name: string;
  legal_name: string;
  siret: string;
  // codes type:code de `territory:perimeter` (aom:…, epci:…, com:…)
  codes: string[];
  // clés des opérateurs qui déclarent des trajets sur ce territoire
  operators: string[];
  campaigns: SyntheticCampaign[];
}

export interface SyntheticUser {
  email: string;
  firstname: string;
  lastname: string;
  role: string;
  // SIREN renvoyé par ProConnect pour ce compte (ignoré pour registry.admin)
  login_siren?: string;
  operator?: string;
  territory?: string;
}

export interface SyntheticProfile {
  name: string;
  // graine du générateur : la changer change tout le jeu de données
  seed: string;
  allowedEnvs: string[];
  // date de référence <YYYY-MM-DD> : les trajets couvrent les `months` mois civils qui la précèdent
  referenceDate: string;
  months: number;
  operators: SyntheticOperator[];
  territories: SyntheticTerritory[];
  users: SyntheticUser[];
  carpools: {
    perTerritoryPerMonth: number;
    identitiesPerOperator: number;
    // part des trajets volontairement incohérents (vitesse, identité, doublon)
    anomalyRate: number;
    statusWeights: Partial<Record<CarpoolStatus, number>>;
  };
}
