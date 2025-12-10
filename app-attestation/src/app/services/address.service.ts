import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable, of } from 'rxjs';
import { map, catchError } from 'rxjs/operators';

interface Feature {
  properties: {
    label: string;
  };
}

@Injectable({
  providedIn: 'root',
})
export class AddressService {
  endpoint: string = 'https://api-adresse.data.gouv.fr/search/';

  constructor(private http: HttpClient) {}

  search(term: string): Observable<string[]> {
    if (!term.trim()) return of([]);

    const name = encodeURIComponent(term.trim());

    return this.http.get<{ features: Feature[] }>(`${this.endpoint}?q=${name}&limit=20`).pipe(
      map((res) => res.features.map((f) => f.properties.label)),
      catchError((err) => {
        console.error(err);
        return of([]);
      }),
    );
  }
}
