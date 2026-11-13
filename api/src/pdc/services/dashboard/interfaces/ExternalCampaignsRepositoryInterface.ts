import type { CreateExternalCampaign, UpdateExternalCampaign } from "@/pdc/services/dashboard/dto/ExternalCampaigns.ts";

export type ExternalCampaignStatus = "draft" | "validated";

export type ExternalCampaignRow = CreateExternalCampaign & {
  id: number;
  created_by_operator_id: number | null;
  created_by_territory_id: number | null;
  status: ExternalCampaignStatus;
  validated_at: Date | null;
  created_at: Date;
  updated_at: Date;
};

export type ExternalCampaignAuthor = { operator_id: number | null; territory_id: number | null };

export type ResolvedTerritory = Pick<CreateExternalCampaign, "territory_type" | "territory_code" | "territory_label">;

export type ExternalCampaignsFilter = {
  id?: number;
  // campagnes où cet opérateur est listé
  operator_id?: number;
  // campagnes saisies par ce territoire ou portant sur lui
  territory?: { id: number; resolved: Pick<ResolvedTerritory, "territory_type" | "territory_code"> | null };
};

export interface ExternalCampaignsRepositoryInterface {
  list(params: ExternalCampaignsFilter): Promise<ExternalCampaignRow[]>;
  find(id: number): Promise<ExternalCampaignRow | null>;
  create(data: CreateExternalCampaign, author: ExternalCampaignAuthor): Promise<{ id: number }>;
  update(data: UpdateExternalCampaign): Promise<void>;
  delete(id: number): Promise<void>;
  validate(id: number, userId: number | null): Promise<void>;
  resolveTerritory(territoryId: number): Promise<ResolvedTerritory | null>;
}

export abstract class ExternalCampaignsRepositoryInterfaceResolver implements ExternalCampaignsRepositoryInterface {
  async list(_params: ExternalCampaignsFilter): Promise<ExternalCampaignRow[]> {
    throw new Error();
  }
  async find(_id: number): Promise<ExternalCampaignRow | null> {
    throw new Error();
  }
  async create(_data: CreateExternalCampaign, _author: ExternalCampaignAuthor): Promise<{ id: number }> {
    throw new Error();
  }
  async update(_data: UpdateExternalCampaign): Promise<void> {
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
