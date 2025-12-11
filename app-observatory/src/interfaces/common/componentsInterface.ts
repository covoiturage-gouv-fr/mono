import { Button } from "../cms/collectionsInterface";

export interface ShareProps {
  name:string, 
  icon:string, 
  href:string,
}

export interface BlockProps {
  title: string,
  content: string,
  img?: string,
  alt?:string,
  buttons?: Button[],
}

export interface HeroProps extends BlockProps{
  subtitle?:string,
}

export interface HighlightProps {
  title: string,
  content: string,
  img?: string,
  alt?: string,
  classes? : Partial<Record<"content" | "root", string>> | undefined,
  buttons?: Button[],
}

export interface MediaProps {
  title: string,
  content: string,
  position?: 'right' | 'left',
  img?: string,
  alt?: string,
  buttons?: Button[],
}

export interface PaginationProps {
  count: number,
  defaultPage?: number,
  href: string
}