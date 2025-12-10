import { DefaultNotification, DefaultTemplateData } from "@/pdc/providers/notification/index.ts";

const defaultData: Partial<DefaultTemplateData> = {
  hero_alt: "Export des données",
  hero_image_src: "https://x0zwu.mjt.lu/tplimg/x0zwu/b/x5zwm/vkxn4.png",
  action_message: "Télécharger le fichier",
  title: "Export des données",
  preview: "Votre export des trajets est disponible.",
  message_html: `
<p>
Votre export des trajets est disponible.
Vous pouvez le télécharger en cliquant sur le bouton ci-dessous.
</p>      
<p>
Les données sont au format CSV compressé dans un fichier ZIP.
</p>      
    `,
  message_text: `
Votre export des trajets est disponible.
Vous pouvez le télécharger en cliquant sur le bouton ci-dessous.

Les données sont au format CSV compressé dans un fichier ZIP.
    `,
};

export class ExportCSVNotification extends DefaultNotification {
  static override readonly subject = "Export des données";
  constructor(to: string, data: Partial<DefaultTemplateData>) {
    super(to, {
      ...defaultData,
      ...data,
    });
  }
}
