import { Button } from '../cms/collectionsInterface'

export interface RowsProps {
  data: any[],
};

export interface VitrineCardProps {
  title: string,
  content: string,
  img: string,
  tags?: string[],
  horizontal?: false | undefined,
  buttons?:Button[]
}

