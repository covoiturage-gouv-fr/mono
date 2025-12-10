import { HttpClient } from "@angular/common/http";
import { Injectable } from "@angular/core";
import { saveAs } from "file-saver";
import { PDFDocument, rgb, StandardFonts } from "pdf-lib";
import { from, ObservableInput, Observer } from "rxjs";
import { catchError, switchMap } from "rxjs/operators";
import { certFilename } from "../shared/helpers/certFilename.helper";
import { format } from "../shared/helpers/date.helper";
import { ProfileLimitedFormInterface } from "../shared/interfaces/ProfileLimitedForm.interface";

@Injectable({
  providedIn: "root",
})
export class PdfLimitedGeneratorService {
  constructor(private http: HttpClient) {}

  generate(data: ProfileLimitedFormInterface, obs?: { onComplete?: Function; onError?: Function }) {
    const observer: Observer<ArrayBuffer> = {
      next: (pdfBytes: ArrayBuffer) => {
        saveAs(new Blob([pdfBytes], { type: "application/pdf" }), certFilename(data.name));
      },
      error: () => {},
      complete: () => {},
    };

    if (obs?.onError) observer["error"] = obs.onError as (err: any) => void;
    if (obs?.onComplete) observer["complete"] = obs.onComplete as () => void;

    this.http
      .get("/assets/certificate_ltd.pdf", { responseType: "arraybuffer" })
      .pipe(
        catchError(this.handleError),
        // convert the PDF load document to an observable
        switchMap((pdfBuffer: ArrayBuffer) => from(PDFDocument.load(pdfBuffer))),
        // embed the font (Promise) and draw all text boxes
        // doc.save returns a Promise with a ArrayBuffer
        switchMap(async (doc: PDFDocument) => {
          const font = await doc.embedFont(StandardFonts.HelveticaBold);
          const page = doc.getPage(0);
          const draw = ((p, f) => (text: string, x: number, y: number) => {
            p.drawText(text, {
              x,
              y,
              size: 11,
              font: f,
              color: rgb(0, 0, 0),
            });
          })(page, font);

          draw(data.name, 80, 650);
          draw(data.address, 140, 620);
          draw(data.employer, 140, 589);
          draw(`${data.workshare}%`, 220, 559);

          // tick checkbox and set value
          // or mask the whole line
          if (data.distance) {
            draw("x", 128, 401.5);
            draw(data.distance.toString(), 418, 402);
          } else {
            page.drawRectangle({
              x: 125,
              y: 442,
              width: 400,
              height: 18,
              color: rgb(1, 1, 1),
            });
          }

          // tick checkbox and set value
          // or mask the whole line
          if (data.days) {
            draw(data.days.toString(), 225, 381);
            draw("x", 128, 381);
          } else {
            page.drawRectangle({
              x: 125,
              y: 377,
              width: 400,
              height: 18,
              color: rgb(1, 1, 1),
            });
          }

          // hide the whole section when no data
          if (!data.distance && !data.days) {
            page.drawRectangle({
              x: 70,
              y: 425,
              width: 140,
              height: 18,
              color: rgb(1, 1, 1),
            });
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

          return doc.save();
        }),
      )
      .subscribe(observer);
  }

  private handleError(err: any): ObservableInput<any> {
    console.error(err);
    return err;
  }
}
