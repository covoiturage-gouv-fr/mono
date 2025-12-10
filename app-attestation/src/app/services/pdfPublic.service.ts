import { HttpClient } from "@angular/common/http";
import { Injectable } from "@angular/core";
import { saveAs } from "file-saver";
import { PDFDocument, rgb, StandardFonts } from "pdf-lib";
import { from, ObservableInput, Observer } from "rxjs";
import { catchError, switchMap } from "rxjs/operators";
import { certFilename } from "../shared/helpers/certFilename.helper";
import { format } from "../shared/helpers/date.helper";
import { ProfilePublicFormInterface } from "../shared/interfaces/ProfilePublicForm.interface";

@Injectable({
  providedIn: "root",
})
export class PdfPublicGeneratorService {
  constructor(private http: HttpClient) {}

  generate(data: ProfilePublicFormInterface, obs?: { onComplete?: Function; onError?: Function }) {
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
      .get("/assets/certificate_public.pdf", { responseType: "arraybuffer" })
      .pipe(
        catchError(this.handleError),
        // convert the PDF load document to an observable
        switchMap((pdfBuffer: ArrayBuffer) => from(PDFDocument.load(pdfBuffer))),
        // embed the font (Promise) and draw all text boxes
        // doc.save returns a Promise with a ArrayBuffer
        switchMap(async (doc: PDFDocument) => {
          const font = await doc.embedFont(StandardFonts.HelveticaBold);
          const page = doc.getPage(0);
          const draw = (
            (p, f) => (text: string, x: number, y: number, size = 11) => {
              p.drawText(text, {
                x,
                y,
                size,
                font: f,
                color: rgb(0, 0, 0),
              });
            }
          )(page, font);

          draw(data.name, 420, 494);
          draw(data.days.toString(), 457, 350);
          draw(data.year.toString(), 568, 350);
          draw(data.location, 380, 245);

          const now = new Date();
          draw(`${format(now.getDate())}/${format(now.getMonth() + 1)}/${now.getFullYear()}`, 380, 232);

          draw(data.name, 79, 482, 10);
          page.drawText(data.ministry, {
            font,
            x: 79,
            y: 456,
            size: 10,
            color: rgb(0, 0, 0),
            maxWidth: 256,
            lineHeight: 13,
          });
          draw(data.rank, 79, 417, 10);

          page.drawText(data.home_address, {
            font,
            x: 79,
            y: 391,
            size: 10,
            color: rgb(0, 0, 0),
            maxWidth: 256,
            lineHeight: 13,
          });

          page.drawText(data.work_address, {
            font,
            x: 79,
            y: 352,
            size: 10,
            color: rgb(0, 0, 0),
            maxWidth: 256,
            lineHeight: 13,
          });

          page.drawText(`${data.workshare}%`, {
            font,
            x: 210,
            y: 311,
            size: 10,
            color: rgb(0, 0, 0),
            maxWidth: 256,
            lineHeight: 13,
          });

          // yes / no check
          if (data.mobility !== "no") {
            draw("x", 94, 286);
            draw(data.mobility_date, 230, 286, 10);
          } else draw("x", 94, 273);

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
