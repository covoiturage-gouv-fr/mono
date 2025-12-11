import { Button } from "../cms/collectionsInterface";

export interface RessourceCardProps {
  title: string;
  content: string;
  date: string;
  img: string;
  href: string;
  link?: string;
  file?: string;
  horizontal?: false;
  buttons?: Button[];
}
