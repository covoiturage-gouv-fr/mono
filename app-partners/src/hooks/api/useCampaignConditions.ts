import { getApiUrl } from "@/helpers/api";
import {
  type CampaignConditionApiRow,
  type CampaignConditionForm,
  fromApiRow,
  toApiPayload,
} from "@/helpers/campaignCondition";
import { apiErrorMessage, parseBody, signalUnauthorized, toUserError } from "@/hooks/useApi";
import { useMemo } from "react";
import { useRestQuery } from "./useRestQuery";

export function useCampaignConditions(params: { operator_id?: number; territory_id?: number } = {}) {
  const query = useRestQuery<CampaignConditionApiRow[]>("v3", "dashboard/campaign-conditions", params, {}, [
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
      throw new Error(apiErrorMessage(response.status, parseBody(await response.text())));
    }
  } catch (e) {
    throw toUserError(e);
  }
}

export const saveCampaignCondition = (form: CampaignConditionForm, id: number | null) =>
  id === null
    ? send("POST", "dashboard/campaign-condition", toApiPayload(form))
    : send("PUT", "dashboard/campaign-condition", { ...toApiPayload(form), id });

export const validateCampaignCondition = (id: number) => send("POST", "dashboard/campaign-condition/validate", { id });

export const deleteCampaignCondition = (id: number) => send("DELETE", `dashboard/campaign-condition/${id}`);
