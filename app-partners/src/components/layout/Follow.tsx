import { fr } from "@codegouvfr/react-dsfr";
import ButtonsGroup from "@codegouvfr/react-dsfr/ButtonsGroup";

export function Follow() {
  return (
    <div className={fr.cx("fr-follow", "fr-mt-10v")}>
      <div className={fr.cx("fr-container")}>
        <div className={fr.cx("fr-grid-row")}>
          <div className={fr.cx("fr-col-12", "fr-col-md-8")}>
            <div className={fr.cx("fr-follow__newsletter")}>
              <div>
                <h2 className={fr.cx("fr-h5", "fr-follow__title")}>
                  Une question ? Écrivez-nous !
                </h2>
                <p>
                  Nous vous répondrons au plus vite ou vous redirigerons vers le
                  bon service
                </p>
                <ButtonsGroup
                  buttons={[
                    {
                      children: "Poser une question",
                      iconId: "fr-icon-mail-line",
                      linkProps: {
                        href: "mailto:contact@covoiturage.beta.gouv.fr",
                        title: "Nous contacter - mailto",
                        "aria-label": "Nous contacter - mailto",
                        target: "_blank",
                      },
                    },
                  ]}
                  buttonsEquisized
                />
              </div>
            </div>
          </div>
          <div className={fr.cx("fr-col-12", "fr-col-md-4")}>
            <div className={fr.cx("fr-follow__social")}>
              <h2 className={fr.cx("fr-h5", "fr-follow__title")}>
                Suivez-nous sur les réseaux sociaux
              </h2>
              <ul className={fr.cx("fr-btns-group")}>
                <li>
                  <a
                    className={fr.cx("fr-btn", "fr-btn--twitter-x")}
                    target="_blank"
                    href="https://twitter.com/Covoitbetagouv"
                    title="Twitter - nouvelle fenêtre"
                    aria-label="Twitter - nouvelle fenêtre"
                  >
                    Twitter
                    <span className={fr.cx("fr-sr-only")}>
                      {" "}
                      - nouvelle fenêtre
                    </span>
                  </a>
                </li>
                <li>
                  <a
                    className={fr.cx("fr-btn--linkedin", "fr-btn")}
                    target="_blank"
                    href="https://www.linkedin.com/company/registre-de-preuve-de-covoiturage"
                    title="LinkedIn - nouvelle fenêtre"
                    aria-label="LinkedIn - nouvelle fenêtre"
                  >
                    LinkedIn
                    <span className={fr.cx("fr-sr-only")}>
                      {" "}
                      - nouvelle fenêtre
                    </span>
                  </a>
                </li>
                <li>
                  <a
                    className={fr.cx("fr-btn--youtube", "fr-btn")}
                    target="_blank"
                    href="https://www.youtube.com/channel/UC-Dge-XxJCIRG22jcGM-VtA"
                    title="YouTube - nouvelle fenêtre"
                    aria-label="YouTube - nouvelle fenêtre"
                  >
                    YouTube
                    <span className={fr.cx("fr-sr-only")}>
                      {" "}
                      - nouvelle fenêtre
                    </span>
                  </a>
                </li>
                <li>
                  <a
                    className={fr.cx("fr-btn--github", "fr-btn")}
                    target="_blank"
                    href="https://github.com/covoiturage-gouv-fr/mono"
                    title="GitHub - nouvelle fenêtre"
                    aria-label="GitHub - nouvelle fenêtre"
                  >
                    Github
                    <span className={fr.cx("fr-sr-only")}>
                      {" "}
                      - nouvelle fenêtre
                    </span>
                  </a>
                </li>
              </ul>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
