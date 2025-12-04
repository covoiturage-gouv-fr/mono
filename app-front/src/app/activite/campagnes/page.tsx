"use client";
import { fr } from "@codegouvfr/react-dsfr";
import Alert from "@codegouvfr/react-dsfr/Alert";
import Button from "@codegouvfr/react-dsfr/Button";
import Input from "@codegouvfr/react-dsfr/Input";
import Pagination from "@codegouvfr/react-dsfr/Pagination";
import Table from "@codegouvfr/react-dsfr/Table";
import { usePathname, useRouter } from "next/navigation";
import { ReactNode, useEffect, useMemo, useState } from "react";
import Loading from "../../../components/layout/Loading";
import { Config } from "../../../config";
import { getApiUrl } from "../../../helpers/api";
import { useApi } from "../../../hooks/useApi";
import { useUrlSearch } from "../../../hooks/useUrlSearch";
import { TerritoriesInterface } from "../../../interfaces/dataInterface";
import { useAuth } from "../../../providers/AuthProvider";

export default function TabCampaigns() {
  const [campaignId, setCampaignId] = useState<number>();
  const { user } = useAuth();
  const { search, debouncedSearch, onChangeSearch: setSearchValue } = useUrlSearch();
  const router = useRouter();
  const pathname = usePathname();
  const pageSize = 15;
  const [page, setPage] = useState(1);
  const url = `${Config.get<string>("auth.domain")}/rpc?methods=campaign:list`;
  const onChangeSearch = (search: string) => {
    setSearchValue(search);
  };
  const init = useMemo(() => {
    const params = {
      ...(user?.territory_id && { territory_id: user?.territory_id }),
      ...(user?.operator_id && { operator_id: user?.operator_id }),
      ...(debouncedSearch && { search: debouncedSearch }),
    };
    return {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        jsonrpc: "2.0",
        method: "campaign:list",
        params: params,
        id: 1,
      }),
    };
  }, [user?.territory_id, user?.operator_id, debouncedSearch]);
  const { data, loading } = useApi<{
    id: number;
    result: { meta: null; data: Record<string, string | number>[] };
    jsonrpc: string;
  }>(url, false, init);
  const territoriesApiUrl = getApiUrl("v3", `dashboard/territories?limit=200`);
  const { data: territoriesData } = useApi<TerritoriesInterface>(
    territoriesApiUrl,
    false,
    undefined,
    user?.territory_id,
  );
  const territoriesList = () => {
    if (user?.territory_id && territoriesData?.data) {
      return [territoriesData?.data.find((t) => t._id === user?.territory_id)] as TerritoriesInterface["data"];
    }
    return territoriesData?.data ?? [];
  };
  const getIcon = (value: string) => {
    return value === "finished" ? (
      <span className={fr.cx("ri-close-circle-fill", "fr-badge--error")} aria-hidden="true"></span>
    ) : value === "active" ? (
      <span className={fr.cx("ri-verified-badge-fill", "fr-badge--success")} aria-hidden="true"></span>
    ) : (
      value
    );
  };
  const active = data?.result.data
    .filter((d) => d.status === "active")
    .sort((a, b) => new Date(b.start_date).getTime() - new Date(a.start_date).getTime());

  const others = data?.result.data
    .filter((d) => d.status !== "active")
    .sort((a, b) => new Date(b.start_date).getTime() - new Date(a.start_date).getTime());
  const dataTableFull = [...(active ?? []), ...(others ?? [])].map((d, i) => [
    getIcon(d.status as string),
    new Date(d.start_date).toLocaleDateString(),
    new Date(d.end_date).toLocaleDateString(),
    territoriesList().find((t) => t._id === d.territory_id)?.name ?? d.territory_id,
    d.name,
    `${(Number(d.incentive_sum) / 100).toLocaleString()} €`,
    `${(Number(d.max_amount) / 100).toLocaleString()} €`,
    <Button
      key={i}
      size="small"
      linkProps={{
        href: "/activite/campagnes/details/" + d._id,
      }}
    >
      Détails
    </Button>,
  ]) as ReactNode[][];
  const pageCount = Math.max(1, Math.ceil(dataTableFull.length / pageSize));
  const dataTable = dataTableFull.slice((page - 1) * pageSize, page * pageSize);
  const totalRecords = dataTableFull.length;

  // ⚠️ si les données changent, on revient à la première page
  useEffect(() => {
    setPage(1);
  }, [dataTableFull.length]);

  const headers = [
    "Statut",
    "Date de début",
    "Date de fin",
    "Territoire",
    "Nom de la campagne",
    "Dépense estimée",
    "Budget",
    "",
  ];

  useEffect(() => {
    if (campaignId) {
      router.push(`/activite/campagnes/details/${campaignId}`);
    } else if (pathname === "/activite") {
      router.push("/activite/campagnes");
    }
  }, [router, campaignId]);

  if (loading) return <Loading />;
  return (
    <>
      {!campaignId && (
        <>
          {!user?.territory_id ? (
            <div
              style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-end", marginBottom: "1rem" }}
            >
              <h3 className={fr.cx("fr-callout__title")}>Campagnes d'incitation</h3>
              <Input
                label="Rechercher"
                state={search !== "" ? (totalRecords <= 0 ? "error" : "success") : "default"}
                stateRelatedMessage={totalRecords + " résultats"}
                hintText="Nom de la campagne / Territoire"
                nativeInputProps={{
                  type: "text",
                  value: search ?? "",
                  onChange: (e) => onChangeSearch(e.target.value),
                }}
              />
            </div>
          ) : null}
          {dataTable.length > 0 || search !== "" ? (
            <>
              <Table data={dataTable} headers={headers} colorVariant="blue-ecume" fixed />
              <div className={fr.cx("fr-grid-row", "fr-mt-5w")}>
                <div className={fr.cx("fr-mx-auto")}>
                  <Pagination
                    defaultPage={page}
                    count={pageCount}
                    getPageLinkProps={(value) => ({
                      onClick: () => setPage(value),
                      href: "#",
                    })}
                    showFirstLast
                  />
                </div>
              </div>
            </>
          ) : (
            <Alert
              title={"Pas de campagnes en cours"}
              severity="info"
              description={
                <p>
                  A date, nous n&apos;effectuons pas le suivi de vos campagnes d&apos;incitations financières,
                  n&apos;hésitez pas à nous contacter en cas de besoin. Vous avez par contre accès à la fonctionnalité
                  d&apos;export de données.
                </p>
              }
            />
          )}
        </>
      )}
    </>
  );
}
