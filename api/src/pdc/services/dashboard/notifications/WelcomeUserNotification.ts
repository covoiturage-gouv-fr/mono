import { DefaultNotification, DefaultTemplateData } from "@/pdc/providers/notification/index.ts";

export interface WelcomeUserTemplateData extends DefaultTemplateData {
  email: string;
  siret: string;
  action_href: string;
}

const title = "Confirmation de la création de votre compte utilisateur sur l'espace partenaire";
const docUrl =
  "https://doc.covoiturage.beta.gouv.fr/nos-services/le-registre-de-preuve-de-covoiturage/comment-fonctionne-lespace-partenaire/ouvrir-son-compte";

export class WelcomeUserNotification extends DefaultNotification {
  static override readonly subject = title;

  constructor(to: string, data: Partial<WelcomeUserTemplateData>) {
    const siretHtml = data.siret ? "<p>SIRET de connexion fourni : {{ siret }}</p>" : "";
    const siretText = data.siret ? "SIRET de connexion fourni : {{ siret }}\n" : "";
    super(to, {
      title,
      fullname: data.email,
      preview: "Votre compte sur l'espace partenaire du Registre de preuve de covoiturage est créé.",
      action_message: "Je me connecte avec ProConnect",
      message_html: `
<p>Un compte utilisateur vient d'être créé pour vous sur l'espace partenaire du Registre de preuve de covoiturage.</p>
${siretHtml}<p>Adresse e-mail du compte : {{ email }}</p>
<p>Connectez-vous avec ProConnect en utilisant cette adresse e-mail. Pour plus d'informations, consultez <a href="${docUrl}">la documentation</a>.</p>`,
      message_text:
        `Un compte utilisateur vient d'être créé pour vous sur l'espace partenaire du Registre de preuve de covoiturage.
${siretText}Adresse e-mail du compte : {{ email }}

Connectez-vous avec ProConnect en utilisant cette adresse e-mail. Documentation : ${docUrl}`,
      ...data,
    });
  }
}
