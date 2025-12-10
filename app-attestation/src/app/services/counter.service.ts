import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { environment } from '../../environments/environment';

@Injectable({ providedIn: 'root' })
export class CounterService {
  private apiUrl = environment.apiUrl || 'http://localhost:8080';
  private allowedOrigins = environment.allowedOrigins || ['http://localhost:4200'];

  constructor(private http: HttpClient) {}

  async save(origin: string, type: 'public' | 'limited', employer: string): Promise<void> {
    if (this.allowedOrigins.indexOf(origin) === -1) {
      return console.error('[honor save] called from forbidden origin', origin);
    }

    this.http.post(`${this.apiUrl}/honor`, { type, employer }).subscribe(() => {}, console.info);
  }
}
