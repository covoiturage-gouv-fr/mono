'use client'
import { CategorieProps } from "@/interfaces/actualites/componentsInterface";
import { fr } from "@codegouvfr/react-dsfr";
import { Tag } from "@codegouvfr/react-dsfr/Tag";
import { useRouter } from 'next/navigation';
import { useEffect, useState } from 'react';

export default function CategoryTags(props: {categories: CategorieProps[], active?:string, type?: 'actualites' | 'ressources', page?: string}) {  
  const router = useRouter();
  const [active, setActive] = useState<string | null>(props.active || null);
  const [page, setPage] = useState<string | null>(props.page || null);
  useEffect(() => {
    const type = props.type || 'actualites';
    const baseRoute = `/${type}`;
    const categoryRoute = active ? `/categorie/${active}` : '';
    const pageRoute = page ? `/page/${page}` : '';

    router.push(`${baseRoute}${categoryRoute}${pageRoute}`);
  }, [active, page, props.type, router]);
  return (
    <ul className={fr.cx('fr-tags-group')}>
      {props.categories &&
        props.categories.map((t) => {
          return (
            <li key={t.id}>
              <Tag
                nativeButtonProps={{
                  onClick: () => {
                     if(t.attributes.slug === active){ 
                      setActive(null)
                      } else {
                      setActive(t.attributes.slug)
                      setPage(null)
                    }
                  }
                }}
                pressed={ t.attributes.slug === active ? true : false}
              >
                {t.attributes.label}
              </Tag>
            </li>
          )
        })
      }
    </ul>
  )
}