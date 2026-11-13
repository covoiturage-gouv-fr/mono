import { getApiUrl } from "@/helpers/api";
import { campaignErrorMessage } from "@/helpers/externalCampaignErrors";
import {
  type ExternalCampaignApiRow,
  type ExternalCampaignForm,
  fromApiRow,
  toApiPayload,
} from "@/helpers/externalCampaign";
import { parseBody, signalUnauthorized, toUserError } from "@/hooks/useApi";
import { useMemo } from "react";
import { useRestQuery } from "./useRestQuery";

export function useExternalCampaigns(params: { id?: number; operator_id?: number; territory_id?: number } = {}) {
  const query = useRestQuery<ExternalCampaignApiRow[]>("v3", "dashboard/external-campaigns", params, {}, [
    params.id,
    params.operator_id,
    params.territory_id,
  ]);
  const rows = useMemo(() => (query.data ?? []).map(fromApiRow), [query.data]);
  return { ...query, rows };
}

async function send(method: "POST" | "PUT" | "DELETE", path: string, body?: unknown): Promise<void> {
  try {
    const response = await fetch(getApiUrl("v3", path), {
      method,
      credentials: "include",
      headers: { "Content-Type": "application/json" },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    if (!response.ok) {
      if (response.status === 401) signalUnauthorized();
      throw new Error(campaignErrorMessage(response.status, parseBody(await response.text())));
    }
  } catch (e) {
    throw toUserError(e);
  }
}

// territoryId : territoire simulé par l'équipe RPC, à la création seulement.
export const saveExternalCampaign = (form: ExternalCampaignForm, id: number | null, territoryId?: number) =>
  id === null
    ? send("POST", "dashboard/external-campaign", { ...toApiPayload(form), territory_id: territoryId })
    : send("PUT", "dashboard/external-campaign", { ...toApiPayload(form), id });

export const validateExternalCampaign = (id: number) => send("POST", "dashboard/external-campaign/validate", { id });

export const deleteExternalCampaign = (id: number) => send("DELETE", `dashboard/external-campaign/${id}`);
