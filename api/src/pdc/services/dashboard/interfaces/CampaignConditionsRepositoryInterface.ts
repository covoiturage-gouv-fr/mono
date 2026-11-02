import type {
  CreateCampaignCondition,
  UpdateCampaignCondition,
} from "@/pdc/services/dashboard/dto/CampaignConditions.ts";

export type CampaignConditionStatus = "draft" | "validated";

export type CampaignConditionRow = CreateCampaignCondition & {
  id: number;
  created_by_operator_id: number | null;
  created_by_territory_id: number | null;
  status: CampaignConditionStatus;
  validated_at: Date | null;
  created_at: Date;
  updated_at: Date;
};

export type CampaignConditionAuthor = { operator_id: number | null; territory_id: number | null };

export type ResolvedTerritory = Pick<CreateCampaignCondition, "territory_type" | "territory_code" | "territory_label">;

export type CampaignConditionsFilter = {
  id?: number;
  // campagnes où cet opérateur est listé
  operator_id?: number;
  // campagnes saisies par ce territoire ou portant sur lui
  territory?: { id: number; resolved: Pick<ResolvedTerritory, "territory_type" | "territory_code"> | null };
};

export interface CampaignConditionsRepositoryInterface {
  list(params: CampaignConditionsFilter): Promise<CampaignConditionRow[]>;
  find(id: number): Promise<CampaignConditionRow | null>;
  create(data: CreateCampaignCondition, author: CampaignConditionAuthor): Promise<{ id: number }>;
  update(data: UpdateCampaignCondition): Promise<void>;
  delete(id: number): Promise<void>;
  validate(id: number, userId: number | null): Promise<void>;
  resolveTerritory(territoryId: number): Promise<ResolvedTerritory | null>;
}

export abstract class CampaignConditionsRepositoryInterfaceResolver implements CampaignConditionsRepositoryInterface {
  async list(_params: CampaignConditionsFilter): Promise<CampaignConditionRow[]> {
    throw new Error();
  }
  async find(_id: number): Promise<CampaignConditionRow | null> {
    throw new Error();
  }
  async create(_data: CreateCampaignCondition, _author: CampaignConditionAuthor): Promise<{ id: number }> {
    throw new Error();
  }
  async update(_data: UpdateCampaignCondition): Promise<void> {
    throw new Error();
  }
  async delete(_id: number): Promise<void> {
    throw new Error();
  }
  async validate(_id: number, _userId: number | null): Promise<void> {
    throw new Error();
  }
  async resolveTerritory(_territoryId: number): Promise<ResolvedTerritory | null> {
    throw new Error();
  }
}
