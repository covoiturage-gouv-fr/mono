import { saveAs } from "file-saver";
import { PDFDocument, rgb, StandardFonts } from "pdf-lib";
import { certFilename } from "../shared/helpers/certFilename.helper";
import { format } from "../shared/helpers/date.helper";
import { PublicFormValues } from "../shared/schemas/publicForm.schema";

export async function generatePublicPdf(data: PublicFormValues): Promise<void> {
  const res = await fetch("/certificate_public.pdf");
  const pdfBuffer = await res.arrayBuffer();

  const doc = await PDFDocument.load(pdfBuffer);
  const font = await doc.embedFont(StandardFonts.HelveticaBold);
  const page = doc.getPage(0);

  const draw = (text: string, x: number, y: number, size = 11) => {
    page.drawText(text, { x, y, size, font, color: rgb(0, 0, 0) });
  };

  draw(data.name, 420, 494);
  draw((data.days ?? "").toString(), 457, 350);
  draw(data.year.toString(), 568, 350);
  draw(data.location, 380, 245);

  const now = new Date();
  draw(`${format(now.getDate())}/${format(now.getMonth() + 1)}/${now.getFullYear()}`, 380, 232);

  draw(data.name, 79, 482, 10);
  page.drawText(data.ministry, { font, x: 79, y: 456, size: 10, color: rgb(0, 0, 0), maxWidth: 256, lineHeight: 13 });
  draw(data.rank, 79, 417, 10);

  page.drawText(data.home_address, { font, x: 79, y: 391, size: 10, color: rgb(0, 0, 0), maxWidth: 256, lineHeight: 13 });
  page.drawText(data.work_address, { font, x: 79, y: 352, size: 10, color: rgb(0, 0, 0), maxWidth: 256, lineHeight: 13 });
  page.drawText(`${data.workshare}%`, { font, x: 210, y: 311, size: 10, color: rgb(0, 0, 0), maxWidth: 256, lineHeight: 13 });

  // yes / no check
  if (data.mobility !== "no") {
    draw("x", 94, 286);
    draw(data.mobility_date ?? "", 230, 286, 10);
  } else {
    draw("x", 94, 273);
  }

  doc.setTitle("Attestation sur l'honneur de covoiturage");
  doc.setSubject("Attestation sur l'honneur de covoiturage");
  doc.setKeywords(["attestation", "covoiturage"]);
  doc.setProducer("beta.gouv");
  doc.setCreator("");
  doc.setAuthor("Ministère de l'aménagement du territoire et de la décentralisation");

  const pdfBytes = await doc.save();
  saveAs(new Blob([new Uint8Array(pdfBytes)], { type: "application/pdf" }), certFilename(data.name));
}
