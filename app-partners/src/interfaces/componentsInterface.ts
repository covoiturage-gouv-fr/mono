export interface BlockProps {
  title: string;
  content: string;
  img?: string;
  alt?: string;
  buttons?: Button[];
}

export interface Button {
  title: string;
  url: string;
  icon?: string;
  color?: "primary" | "secondary";
}
export interface PaginationProps {
  count: number;
  defaultPage?: number;
  onChange: (id: number) => void;
}
