import { saveAs } from "file-saver";
import { PDFDocument, rgb, StandardFonts } from "pdf-lib";
import { certFilename } from "../shared/helpers/certFilename.helper";
import { format } from "../shared/helpers/date.helper";
import { LimitedFormValues } from "../shared/schemas/limitedForm.schema";

export async function generateLimitedPdf(data: LimitedFormValues): Promise<void> {
  // load the PDF template from /public (served at the site root)
  const res = await fetch("/certificate_ltd.pdf");
  const pdfBuffer = await res.arrayBuffer();

  const doc = await PDFDocument.load(pdfBuffer);
  const font = await doc.embedFont(StandardFonts.HelveticaBold);
  const page = doc.getPage(0);

  const draw = (text: string, x: number, y: number) => {
    page.drawText(text, { x, y, size: 11, font, color: rgb(0, 0, 0) });
  };

  draw(data.name, 80, 650);
  draw(data.address, 140, 620);
  draw(data.employer, 140, 589);
  draw(`${data.workshare}%`, 220, 559);

  // tick checkbox and set value, or mask the whole line
  if (data.distance) {
    draw("x", 128, 401.5);
    draw(data.distance, 418, 402);
  } else {
    page.drawRectangle({ x: 125, y: 442, width: 400, height: 18, color: rgb(1, 1, 1) });
  }

  if (data.days) {
    draw(data.days, 225, 381);
    draw("x", 128, 381);
  } else {
    page.drawRectangle({ x: 125, y: 377, width: 400, height: 18, color: rgb(1, 1, 1) });
  }

  // hide the whole section when no data
  if (!data.distance && !data.days) {
    page.drawRectangle({ x: 70, y: 425, width: 140, height: 18, color: rgb(1, 1, 1) });
  }

  draw(data.location, 120, 239);

  const now = new Date();
  draw(`${format(now.getDate())}/${format(now.getMonth() + 1)}/${now.getFullYear()}`, 120, 218);

  // set metadata
  doc.setTitle("Attestation sur l'honneur de covoiturage");
  doc.setSubject("Attestation sur l'honneur de covoiturage");
  doc.setKeywords(["attestation", "covoiturage"]);
  doc.setProducer("beta.gouv");
  doc.setCreator("");
  doc.setAuthor("Ministère de l'aménagement du territoire et de la décentralisation");

  const pdfBytes = await doc.save();
  saveAs(new Blob([new Uint8Array(pdfBytes)], { type: "application/pdf" }), certFilename(data.name));
}
