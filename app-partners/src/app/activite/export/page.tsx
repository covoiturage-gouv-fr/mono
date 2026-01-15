"use client";
import SelectGeo from "@/components/common/SelectGeo";
import SelectTerritory from "@/components/common/SelectTerritory";
import ExportList from "@/components/export/ExportList";
import { useExportCreate } from "@/hooks/api/useExportCreate";
import { TerritorySelectorsInterface } from "@/interfaces/dataInterface";
import { type PerimeterType } from "@/interfaces/searchInterface";
import { useAuth } from "@/providers/AuthProvider";
import { fr } from "@codegouvfr/react-dsfr";
import { Alert } from "@codegouvfr/react-dsfr/Alert";
import Button from "@codegouvfr/react-dsfr/Button";
import { RadioButtons } from "@codegouvfr/react-dsfr/RadioButtons";
import { DatePicker, LocalizationProvider } from "@mui/x-date-pickers";
import { AdapterDayjs } from "@mui/x-date-pickers/AdapterDayjs";
import dayjs, { type Dayjs } from "dayjs";
import "dayjs/locale/fr";
import { useEffect, useRef, useState } from "react";

export default function TabExport() {
  const { user, simulate, simulatedRole } = useAuth();
  const [territoryId, setTerritoryId] = useState(user?.territory_id);
  const [refreshTrigger, setRefreshTrigger] = useState(0);
  const forceHour = (input: Date | Dayjs, range: "start" | "end"): Date => {
    const d = dayjs(input);
    const date =
      range === "start"
        ? new Date(Date.UTC(d.year(), d.month(), d.date() - 1, 22, 0, 0, 0))
        : new Date(Date.UTC(d.year(), d.month(), d.date(), 21, 59, 59, 999));
    return date;
  };
  const [startDate, setStartDate] = useState(dayjs().subtract(1, "month"));
  const [endDate, setEndDate] = useState(dayjs().subtract(5, "days"));
  const [territorySelectors, setTerritorySelectors] = useState<TerritorySelectorsInterface>();
  const [geoSelector, setGeoSelector] = useState<"geo" | "campaign">("campaign");

  const scrollRef = useRef<HTMLDivElement | null>(null);

  const { createExport, data: exportResponse, loading, error, reset } = useExportCreate();

  useEffect(() => {
    if (exportResponse) {
      setRefreshTrigger((prev) => prev + 1);
      scrollRef.current?.scrollIntoView({ behavior: "smooth" });
    }
  }, [exportResponse]);

  useEffect(() => {
    if (error) {
      scrollRef.current?.scrollIntoView({ behavior: "smooth" });
    }
  }, [error]);

  const handleExport = async () => {
    try {
      await createExport({
        tz: "Europe/Paris",
        start_at: forceHour(startDate, "start"),
        end_at: forceHour(endDate, "end"),
        territory_id: territorySelectors ? [] : territoryId ? [territoryId] : [],
        geo_selector: territorySelectors,
        operator_id: user?.operator_id ? [user.operator_id] : [],
      });
    } catch {
      // Error is handled by the hook's error state
    }
  };
  const onChangeGeo = (
    option: {
      id: string;
      territory: string;
      l_territory: string;
      type: PerimeterType;
    } | null,
  ) => {
    if (option) {
      setTerritorySelectors({
        [option.type]: [option.territory],
      });
    } else {
      setTerritorySelectors({});
    }
  };

  const canChoose = user?.role === "registry.admin" && !simulate;

  const isAdminImpersonatingOperator = user?.role === "registry.admin" && simulate && simulatedRole === "operator";

  const showGeoSelector = geoSelector === "geo" || !!user?.role?.startsWith("operator") || isAdminImpersonatingOperator;

  const showCampaignSelector = geoSelector === "campaign" && user?.role === "registry.admin" && !simulate;

  return (
    <>
      <Alert
        title={"Important"}
        severity="info"
        description={
          <ul>
            Les exports sont réalisés sur l’ensemble des trajets respectant les conditions générales d’utilisation de
            covoiturage.beta.gouv et ayant une origine OU destination sur le territoire sélectionné. A noter que :
            <li>
              la colonne “statut” permet d’identifier les trajets validés par le RPC suite à différentes analyses.
            </li>
            <li>
              la définition de chacune des données de l’export est accessible dans notre{" "}
              <a target="_blank" href="https://tech.covoiturage.beta.gouv.fr/topic/topic-export-de-trajets">
                documentation technique
              </a>
            </li>
            <li>enfin, pour les collectivités, le périmètre de l’export est celui de son territoire géographique</li>
          </ul>
        }
      />
      <div className={fr.cx("fr-mt-4w")}>
        {canChoose && (
          <RadioButtons
            legend="Périmètre de l'export"
            name="radio"
            options={[
              {
                label: "Périmètre géographique",
                nativeInputProps: {
                  checked: geoSelector === "geo",
                  onChange: () => setGeoSelector("geo"),
                },
              },
              {
                label: "Périmètre campagne",
                nativeInputProps: {
                  checked: geoSelector === "campaign",
                  onChange: () => setGeoSelector("campaign"),
                },
              },
            ]}
            orientation="horizontal"
          />
        )}
        <LocalizationProvider dateAdapter={AdapterDayjs} adapterLocale="fr">
          {showGeoSelector && <SelectGeo onChange={onChangeGeo} />}
          {showCampaignSelector && (
            <SelectTerritory
              defaultValue={user.territory_id}
              onChange={(v) => {
                setTerritoryId(v);
              }}
            />
          )}
          {(!!territoryId || !!territorySelectors) && (
            <div className="fr-mt-4w">
              <div
                style={{
                  display: "flex",
                  flexDirection: "row",
                  flexWrap: "wrap",
                  gap: "16px",
                }}
              >
                <DatePicker
                  sx={{
                    maxWidth: "200px",
                  }}
                  label="Début"
                  value={startDate}
                  onChange={(v) => v && setStartDate(v)}
                  minDate={dayjs().subtract(2, "years")}
                  maxDate={endDate}
                />

                <DatePicker
                  sx={{
                    maxWidth: "200px",
                  }}
                  label="Fin"
                  value={endDate}
                  onChange={(v) => v && setEndDate(v)}
                  minDate={startDate}
                  maxDate={dayjs().subtract(5, "days")}
                />
              </div>

              <div>
                <Button
                  disabled={loading}
                  style={{
                    marginTop: fr.spacing("5v"),
                  }}
                  className="fr-btn"
                  size="large"
                  onClick={() => void handleExport()}
                >
                  Exporter
                </Button>
                <div ref={scrollRef}>
                  {error && (
                    <Alert
                      style={{
                        marginTop: fr.spacing("5v"),
                      }}
                      closable
                      onClose={reset}
                      severity="warning"
                      title="Une erreur est survenue"
                      description={error.message}
                    />
                  )}
                  {exportResponse && (
                    <Alert
                      style={{
                        marginTop: fr.spacing("5v"),
                      }}
                      severity="success"
                      title="Succès"
                      description={
                        <>
                          Vous allez recevoir un email avec le lien de téléchargement d'ici maximum 2h.
                          <br />
                          Un rafraîchissement de la page sera nécessaire pour mettre à jour le statut de traitement de
                          l'export affiché ci-dessous.
                        </>
                      }
                      closable
                      onClose={reset}
                    />
                  )}
                </div>
              </div>
            </div>
          )}
        </LocalizationProvider>
      </div>

      <ExportList refreshTrigger={refreshTrigger} days={30} pageSize={10} />
    </>
  );
}
