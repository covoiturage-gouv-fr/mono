import { Observable } from 'rxjs';

export interface SearchServiceInterface {
  search(term: string): Observable<string[]>;
}
