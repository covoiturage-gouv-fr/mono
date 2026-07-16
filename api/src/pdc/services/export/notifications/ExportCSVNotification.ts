import { DefaultNotification, DefaultTemplateData } from "@/pdc/providers/notification/index.ts";
import { Export } from "../models/Export.ts";

export interface ExportCSVTemplateData extends Pick<Export, "_id" | "uuid" | "target" | "status"> {
  fullname: string;
}

const exportsUrl = "https://partenaire.covoiturage.beta.gouv.fr/activite/export/";

const defaultData: Partial<DefaultTemplateData> = {
  hero_alt: "Export des données",
  hero_image_src: "https://x0zwu.mjt.lu/tplimg/x0zwu/b/x5zwm/vkxn4.png",
  action_message: "Voir mes exports",
  action_href: exportsUrl,
  title: "Export des données",
  preview: "Votre export des trajets est disponible.",
  message_html: `
<p>
Votre export des trajets est disponible.
Retrouvez-le dans la liste de vos exports en cliquant sur le bouton ci-dessous.
</p>
<p>
Les données sont au format CSV compressé dans un fichier ZIP.
</p>
    `,
  message_text: `
Votre export des trajets est disponible.
Retrouvez-le dans la liste de vos exports à l'adresse suivante :
${exportsUrl}

Les données sont au format CSV compressé dans un fichier ZIP.
    `,
};

export class ExportCSVNotification extends DefaultNotification {
  static override readonly subject = "Export des données";
  constructor(to: string, data: Partial<ExportCSVTemplateData>) {
    super(to, { ...defaultData, ...data });
  }
}
