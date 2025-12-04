export type ParamsInterface = {
  days?: number;
};

export type ResultItemInterface = {
  start_date: Date;
  end_date: Date;
  geo_selector: string[];
  download_url: string;
  filename: string;
  file_size: number;
  status: string;
};

export type ResultInterface = {
  meta: null;
  data: ResultItemInterface[];
};

export const handlerConfig = {
  service: "exports",
  method: "list",
} as const;

export const signature = `${handlerConfig.service}:${handlerConfig.method}` as const;
