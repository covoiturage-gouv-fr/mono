import { DefaultNotification, DefaultTemplateData } from "@/pdc/providers/notification/index.ts";
import { Export, ExportError } from "@/pdc/services/export/models/Export.ts";

export interface ExportCSVSupportTemplateData extends Pick<Export, "_id" | "uuid" | "target" | "status"> {
  error: ExportError;
}

const defaultData: Partial<DefaultTemplateData> = {
  hero_alt: "Export des données",
  hero_image_src: "https://x0zwu.mjt.lu/tplimg/x0zwu/b/x5zwm/vkxn4.png",
  title: "Erreur d'export des données",
  preview: "Une erreur s'est produite lors de l'export des données.",
  message_html: `
<p>Une erreur s'est produite lors de l'export des données.</p>
<ul>
  <li>_id: {{ _id }}</li>
  <li>uuid: {{ uuid }}</li>
  <li>target: {{ target }}</li>
  <li>status: {{ status }}</li>
  <li>error: {{ error }}</li>
</ul>
`,
  message_text: `
Une erreur s'est produite lors de l'export des données.

_id: {{ _id }}
uuid: {{ uuid }}
target: {{ target }}
status: {{ status }}
error: {{ error }}
    `,
};

export class ExportCSVSupportNotification extends DefaultNotification {
  static override readonly subject = "Erreur d'export";
  constructor(to: string, data: Partial<DefaultTemplateData>) {
    super(to, { ...defaultData, ...data });
  }
}
