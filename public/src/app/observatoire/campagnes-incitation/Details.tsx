import SectionTitle from "@/components/common/SectionTitle";
import ButtonsGroup from "@codegouvfr/react-dsfr/ButtonsGroup";
import Table from "@codegouvfr/react-dsfr/Table";

export default function Details({ data }: { data: any }) {
  const title = `Campagne d'incitation ${
    data.name ? `pour la collectivité : ${data.name}` : ""
  }`;
  const table1 = [
    data.debut
      ? ["Début", new Date(data.debut).toLocaleDateString()]
      : ["Debut", "date inconnue"],
    data.fin
      ? ["Fin", new Date(data.fin).toLocaleDateString()]
      : ["Fin", "date inconnue"],
    data.budget
      ? ["Budget global", `${Number(data.budget).toLocaleString()} €`]
      : ["Budget global", "inconnu"],
    [
      "Sens des trajets éligibles à l'incitation par rapport à l’AOM",
      data.sens === "ET/OU"
        ? "Origine et/ou destination"
        : data.sens === "ET"
        ? "Origine et destination"
        : "Origine ou destination",
    ],
    data.exclusion
      ? [
          "Zone d'exclusion de la campagne",
          `${data.liste_exclusion} ${
            data.autre_exclusion && data.autre_exclusion !== "Non"
              ? data.autre_exclusion
              : ""
          }`,
        ]
      : ["Zone d'exclusion de la campagne", "aucune"],
  ];
  const table2 = [
    data.operateurs ? ["Opérateur(s)", data.operateurs] : [],
    data.classe ? ["Classes de trajets éligibles", data.classe] : [],
    data.l_min ? ["Distance minimale du trajet", `${data.l_min} km`] : [],
    data.l_max ? ["Distance maximale du trajet", `${data.l_max} km`] : [],
  ];
  const table3 = [
    data.p_gratuit ? ["Gratuité pour le passager", data.p_gratuit] : [],
    data.p_gratuit === "Oui"
      ? ["Passager éligibles", `${data.p_eligible}`]
      : [],
    data.p_tk === "Oui"
      ? [
          "Passager éligible à une réduction du montant du ticket",
          `${data.p_e_tk}`,
        ]
      : [],
    data.p_m_tk ? ["Montant (en €) du ticket passager", data.p_m_tk] : [],
    data.c_m_min_p
      ? [
          "Montant minimum d’incitation du conducteur par passager transporté (en €)",
          data.c_m_min_p,
        ]
      : [],
    data.c_m_max_p
      ? [
          "Montant maximal d’incitation du conducteur par passager transporté (en €)",
          data.c_m_max_p,
        ]
      : [],
  ];
  const table4 = [
    data.p_t_max
      ? [
          "Plafond de trajet incités réalisés par un passager par jour",
          Math.round(data.p_t_max / 30).toLocaleString(),
        ]
      : [],
    data.c_t_max
      ? [
          "Plafond de trajet incités réalisés par un conducteur par jour",
          Math.round(data.c_t_max / 30).toLocaleString(),
        ]
      : [],
    data.c_m_max_p
      ? [
          "Plafond d’incitation mensuel généré par un conducteur (en €)",
          data.c_m_max_p,
        ]
      : [],
  ];

  return (
    <>
      <SectionTitle title={title} />
      <Table
        caption="Information principales da la campagne"
        fixed
        bordered
        colorVariant="blue-ecume"
        data={table1}
        headers={[]}
      />
      <Table
        caption="Critères d'éligibilité des trajets à l'incitation"
        fixed
        bordered
        colorVariant="blue-ecume"
        data={table2}
        headers={[]}
      />
      <Table
        caption="Objectif et dimensionnement de la campagne"
        fixed
        bordered
        colorVariant="blue-ecume"
        data={table3}
        headers={[]}
      />
      <Table
        caption="Limite(s) et seuil(s)"
        fixed
        bordered
        colorVariant="blue-ecume"
        data={table4}
        headers={[]}
      />
      {data.lien && (
        <ButtonsGroup
          alignment="right"
          buttonsEquisized
          buttons={[
            {
              children: "Détails de la campagne",
              iconId: "fr-icon-arrow-right-s-line",
              iconPosition: "right",
              linkProps: {
                href: data.lien,
              },
            },
          ]}
        />
      )}
    </>
  );
}
