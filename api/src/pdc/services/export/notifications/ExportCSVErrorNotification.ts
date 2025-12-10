import { DefaultNotification, DefaultTemplateData } from "@/pdc/providers/notification/index.ts";

const defaultData: Partial<DefaultTemplateData> = {
  hero_alt: "Export des données",
  hero_image_src: "https://x0zwu.mjt.lu/tplimg/x0zwu/b/x5zwm/vkxn4.png",
  title: "Erreur d'export des données",
  preview: "Une erreur s'est produite lors de l'export des données.",
  message_html: `
<p> 
Une erreur s'est produite lors de l'export des données.
L'équipe technique du Registre de preuve de covoiturage a été notifiée.
</p>
<p>
Nous vous invitons à relancer un export ultérieurement ou nous contacter directement à l'adresse suivante
<a href="mailto:technique@covoiturage.beta.gouv.fr">technique@covoiturage.beta.gouv.fr</a>
</p>
    `,
  message_text: `
Une erreur s'est produite lors de l'export des données.
L'équipe technique du Registre de preuve de covoiturage a été notifiée.

Nous vous invitons à relancer un export ultérieurement ou nous contacter par mail <technique@covoiturage.beta.gouv.fr>
    `,
};

export class ExportCSVErrorNotification extends DefaultNotification {
  static override readonly subject = "Erreur d'export des données";
  constructor(to: string, data: Partial<DefaultTemplateData>) {
    super(to, {
      ...defaultData,
      ...data,
    });
  }
}
