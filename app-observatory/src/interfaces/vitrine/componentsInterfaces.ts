import { Button, VitrineRow } from "@/interfaces/cms/strapi"

export interface RowsProps {
  data: VitrineRow[],
};

export interface VitrineCardProps {
  title: string,
  content: string,
  img: string,
  tags?: string[],
  horizontal?: false | undefined,
  buttons?:Button[]
}

