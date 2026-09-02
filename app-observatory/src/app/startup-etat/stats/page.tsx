import PageTitle from "@/components/common/PageTitle";
import Chart from "@/components/observatoire/charts/Chart";
import Rows from "@/components/observatoire/indicators/Rows";
import TrajetsChart from "@/components/vitrine/stats/TrajetsChart";
import TrajetsValidesTotal from "@/components/vitrine/stats/TrajetsValidesTotal";
import { fr } from "@codegouvfr/react-dsfr";
import { Alert } from "@codegouvfr/react-dsfr/Alert";
import { Metadata } from "next";
import Link from "next/link";
import {
  CEE_COURTE_DISTANCE,
  CEE_LONGUE_DISTANCE,
  COUT_UNITAIRE_TRAJET,
  INDICATEURS,
  frMonthLabel,
} from "./data";

export const metadata: Metadata = {
  title: "Mesures d'impact | Covoiturage.beta.gouv.fr",
  description:
    "Mesures d'impact de la Startup d'État covoiturage.beta.gouv.fr : accompagnement des collectivités, qualité de la donnée, déploiement des dispositifs du plan national covoiturage.",
};

const DOC =
  "https://doc.covoiturage.beta.gouv.fr/bienvenue/manifeste/politiques-publiques-en-faveur-du-covoiturage";

function Section({
  title,
  intro,
  children,
}: {
  title: string;
  intro: string;
  children: React.ReactNode;
}) {
  return (
    <section className={fr.cx("fr-mt-6w")}>
      <h2 className={fr.cx("fr-h4")}>{title}</h2>
      <p className={fr.cx("fr-text--sm")}>{intro}</p>
      {children}
    </section>
  );
}

export default function StatsPage() {
  return (
    <div id="content">
      <PageTitle title="Mesures d'impact de la Startup d'État covoiturage.beta.gouv.fr" />

      <Alert
        severity="info"
        title="À noter"
        description="Les mesures d'impact présentées sont issues des données collectées auprès des plateformes de covoiturage courte distance partenaires. Il ne saurait s'agir d'une représentation exhaustive du covoiturage en France."
      />

      <Section
        title="Impact n°1 — Animer et accompagner un écosystème au cœur duquel se trouvent les collectivités"
        intro="covoiturage.beta.gouv.fr fédère les plateformes de covoiturage et outille les collectivités qui déploient des politiques publiques en faveur du covoiturage du quotidien."
      >
        <Rows
          data={[
            INDICATEURS.plateformes_actives,
            INDICATEURS.collectivites_accompagnees,
            INDICATEURS.pct_collectivites_reduiraient_incitations,
          ]}
        />
        <p className={fr.cx("fr-text--sm", "fr-mt-2w")}>
          <Link
            href="https://us02web.zoom.us/webinar/register/WN_Ww5g-l7wQHOBbN9CNeCaZA#/registration"
            target="_blank"
            rel="noopener noreferrer"
          >
            S’inscrire à la prochaine démonstration
          </Link>
          .
        </p>
      </Section>

      <Section
        title="Impact n°2 — Être tiers de confiance sur la qualité de la donnée pour planifier, suivre et mesurer les politiques publiques covoiturage"
        intro="covoiturage.beta.gouv.fr normalise et contrôle les trajets transmis par les plateformes, puis diffuse la donnée en open data pour piloter les politiques publiques."
      >
        <TrajetsValidesTotal />

        <TrajetsChart granularity="month" />
        <p className={fr.cx("fr-hint-text", "fr-mb-4w")}>
          Un « trajet passager » correspond à un couple passager / conducteur : à
          chaque passager est affecté un trajet. Les trajets faisant l’objet
          d’une suspicion de fraude ou ne respectant pas les CGU du Registre de
          preuve de covoiturage sont exclus. L’objectif national du plan
          covoiturage est de 3 millions de trajets quotidiens d’ici 2027.
        </p>

        <TrajetsChart granularity="year" />

        <Chart
          title="Coût unitaire d'un trajet validé par covoiturage.beta.gouv.fr"
          kind="line"
          unit="€"
          labels={COUT_UNITAIRE_TRAJET.map((p) => p.x)}
          data={COUT_UNITAIRE_TRAJET.map((p) => p.y)}
          download={{ data: COUT_UNITAIRE_TRAJET, filename: "cout-unitaire-trajet" }}
        />
        <p className={fr.cx("fr-hint-text", "fr-mb-4w")}>
          Coût de fonctionnement annuel total de covoiturage.beta.gouv.fr divisé
          par le nombre de trajets validés dans l’année : il traduit la
          progression de l’efficience du service dans le temps.
        </p>

        <Rows
          data={[
            INDICATEURS.note_satisfaction_observatoire,
            INDICATEURS.telechargements_datagouv,
          ]}
        />
        <p className={fr.cx("fr-text--sm", "fr-mt-2w")}>
          <Link
            href="https://observatoire.covoiturage.gouv.fr/observatoire/territoire"
            target="_blank"
            rel="noopener noreferrer"
          >
            Consulter l’Observatoire national du covoiturage
          </Link>
          {" — "}
          <Link
            href="https://www.data.gouv.fr/datasets/trajets-realises-en-covoiturage-registre-de-preuve-de-covoiturage"
            target="_blank"
            rel="noopener noreferrer"
          >
            accéder au jeu de données sur data.gouv.fr
          </Link>
          .
        </p>
      </Section>

      <Section
        title="Impact n°3 — Déployer et accélérer les dispositifs du plan national covoiturage du quotidien grâce à des outils dédiés"
        intro="covoiturage.beta.gouv.fr met à disposition les outils qui rendent opérationnels les dispositifs du plan national : forfait mobilités durables, Fonds vert, primes covoiturage."
      >
        <h3 className={fr.cx("fr-h6", "fr-mt-4w")}>
          Le Forfait mobilités durables (FMD)
        </h3>
        <p className={fr.cx("fr-text--sm")}>
          Dispositif financier de soutien aux salariés du secteur privé et aux
          agents publics pour leurs déplacements domicile-travail.{" "}
          <Link href={`${DOC}/f.a.q.-fmd`} target="_blank" rel="noopener noreferrer">
            En savoir plus
          </Link>
          .
        </p>
        <Rows data={[INDICATEURS.attestations_honneur_fmd_total]} />

        <h3 className={fr.cx("fr-h6", "fr-mt-4w")}>Le Fonds vert</h3>
        <p className={fr.cx("fr-text--sm")}>
          Fonds d’accélération de la transition écologique dans les territoires,
          qui soutient les collectivités dans leurs projets en faveur du
          covoiturage.{" "}
          <Link
            href={`${DOC}/f.a.q.-fonds-vert`}
            target="_blank"
            rel="noopener noreferrer"
          >
            En savoir plus
          </Link>
          .
        </p>
        <Rows
          data={[
            INDICATEURS.campagnes_incitation_2024,
            INDICATEURS.lignes_covoiturage_2024,
            INDICATEURS.aires_covoiturage_2024,
          ]}
        />

        <h3 className={fr.cx("fr-h6", "fr-mt-4w")}>
          Les primes de 100 € — covoiturage courte distance
        </h3>
        <p className={fr.cx("fr-text--sm")}>
          Dispositif « coup de pouce » effectif du 01/01/2023 au 31/12/2024,
          suivi d’un dispositif non bonifié depuis janvier 2025.{" "}
          <Link
            href={`${DOC}/f.a.q.-prime-de-100-eur`}
            target="_blank"
            rel="noopener noreferrer"
          >
            En savoir plus
          </Link>
          .
        </p>
        <Chart
          title="Demandes de CEE enregistrées mensuellement par le RPC — covoiturage courte distance"
          labels={CEE_COURTE_DISTANCE.map((p) => frMonthLabel(p.x))}
          data={CEE_COURTE_DISTANCE.map((p) => p.y)}
          download={{ data: CEE_COURTE_DISTANCE, filename: "cee-courte-distance" }}
        />

        <h3 className={fr.cx("fr-h6", "fr-mt-4w")}>
          Les primes CEE de 100 € — covoiturage longue distance
        </h3>
        <p className={fr.cx("fr-text--sm")}>
          Dispositif « coup de pouce » longue distance effectif du 01/01/2023 au
          01/07/2024.{" "}
          <Link
            href="https://www.ecologie.gouv.fr/politiques-publiques/covoiturage-france-ses-avantages-reglementation-vigueur"
            target="_blank"
            rel="noopener noreferrer"
          >
            En savoir plus
          </Link>
          .
        </p>
        <Chart
          title="Demandes de CEE enregistrées mensuellement par le RPC — covoiturage longue distance"
          labels={CEE_LONGUE_DISTANCE.map((p) => frMonthLabel(p.x))}
          data={CEE_LONGUE_DISTANCE.map((p) => p.y)}
          download={{ data: CEE_LONGUE_DISTANCE, filename: "cee-longue-distance" }}
        />
      </Section>

      <p className={fr.cx("fr-hint-text", "fr-mt-6w")}>
        Trajets par mois et par an : données de l’API de l’Observatoire national
        du covoiturage, complétées avant 2019 par un relevé figé. Autres
        indicateurs : dashboard interne de suivi d’impact, relevé le 2 septembre
        2026. Le volume de demandes de CEE d’un mois n’est complet qu’environ
        deux semaines après la fin du mois.
      </p>
    </div>
  );
}
