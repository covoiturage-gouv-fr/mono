import { Button } from "@/interfaces/cms/strapi";

export interface ActuCardProps {
  title: string;
  content: string;
  date: string;
  img: string;
  href: string;
  horizontal?: false | undefined;
  categories?: CategorieProps[];
  buttons?: Button[];
}

export interface CategorieProps {
  id: string | number;
  attributes: {
    label: string;
    slug: string;
  };
}
