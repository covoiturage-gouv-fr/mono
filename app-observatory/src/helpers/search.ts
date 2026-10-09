import { Config } from '@/config';
import { search } from '@/config/search';
import { PerimeterType } from '../interfaces/observatoire/Perimeter';
import { TerritoryListInterface } from '../interfaces/observatoire/dataInterfaces';

export const searchHost = Config.get<string>('search.host');

export const fetchSearchAPI = async (path:string, options = {}) => {
  const mergedOptions = {
    headers: search.headers,
    ...options,
  };
  const requestUrl = `${searchHost}/${path}`;
  let response: Response;
  try {
    response = await fetch(requestUrl, mergedOptions);
  }
  catch(e){
    console.error(e);
    throw new Error(`Please check if your server is running and you set all the required tokens.`);
  }
  // Meilisearch répond en JSON même en erreur : sans ce garde, le corps d'erreur
  // est retourné comme un résultat et casse l'appelant plus loin.
  if (!response.ok) {
    throw new Error(`Search API ${path}: HTTP ${response.status} ${await response.text()}`);
  }
  return response.json();
}

export const castPerimeterType = (value: PerimeterType) => {
  switch (value) {
    case 'com':
      return 'Commune';
    case 'epci':
      return 'Communauté de commune';
    case 'aom':
      return 'Autorité organisatrice des mobilités';
    case 'dep':
      return 'Département';
    case 'reg':
      return 'Région';
    case 'country':
      return 'Pays';
    case 'custom':
      return 'Territoire personnalisé';
  };
}

export const getUrl = (url: string, option?:TerritoryListInterface) => {
  if (!option) return `/observatoire/${url}`;
  const params = new URLSearchParams({ code: option.territory.slice(0,9), type: option.type });
  return `/observatoire/${url}?${params}`;
}