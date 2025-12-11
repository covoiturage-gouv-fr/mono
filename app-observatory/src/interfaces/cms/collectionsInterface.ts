export type Collections = {
  articles: Article,
  ressources: Ressource,
  categories: Category,
  pages: Page,
  hero: Hero,
  highlight: Highlight,
  list: List,
}

export interface Ressource {
  id:string,
  slug:string,
  title:string,
  content?:string,
  img?:string,
  img_legend?:string,
  file:string,
  status:string,
  date_publi: Date,
  date_created: Date,
  date_updated: Date,
  user_created: Date,
}

export interface Category {
  Categories_id: {
    id:string,
    slug:string,
    name:string,
  }
}

export interface Article {
  id:string,
  slug:string,
  title:string,
  description:string,
  categories:Category[],
  content?:string,
  img?:string,
  img_legend?:string,
  status:string,
  date_created: Date,
  date_updated: Date,
  user_created: Date,
}

export interface Page {
  id:string,
  slug:string,
  title:string,
  content?:string,
  img?:string,
  img_legend?:string,
  status:string,
  sections?:Section[]
}

export interface Section {
  collection:string,
  item: Ressource | Hero,
}

export interface Hero {
  id:string,
  title:string,
  subtitle:string,
  content?:string,
  img?:string,
  alt?:string,
  background_color?:string,
  buttons:Button[]
}

export interface Highlight {
  id:string,
  title:string,
  content?:string,
  img?:string,
  alt?:string,
  buttons:Button[]
}

export interface List {
  id:string,
  title:string,
  highlights:Highlight[]
}

export interface Button {
  title: string,
  url: string,
  icon?: string,
  color?:'primary' | 'secondary',
}