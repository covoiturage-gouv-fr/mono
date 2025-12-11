import { PaginationProps } from "@/interfaces/common/componentsInterface";
import { fr } from "@codegouvfr/react-dsfr";
import Link from 'next/link';

export default function Pagination(props:PaginationProps) {
  
  const getPagination = (count:number, defaultPage: number) => {
    const maxVisiblePages = 10;
    // first n pages
    if (count <= maxVisiblePages) {
      return Array.from({ length: count }, (_, v) => ({
        number: v + 1,
        active: defaultPage === v + 1
      }));
    }
    // last n pages
    if (defaultPage > count - maxVisiblePages) {
      return Array.from({ length: maxVisiblePages }, (_, v) => {
        const pageNumber = count - (maxVisiblePages - v) + 1;
        return {
          number: pageNumber,
          active: defaultPage === pageNumber
        };
      });
    }
    return []
  };
  const defaultPage = props.defaultPage ? props.defaultPage : 1;
  const pages = getPagination(props.count, defaultPage);

  return(
    <div className={fr.cx('fr-grid-row', 'fr-mt-5w')}>
      <div className={fr.cx('fr-mx-auto')}>
      <nav role="navigation" className={fr.cx('fr-pagination')} aria-label="Pagination">
        <ul className={fr.cx('fr-pagination__list')}>
          <li>
            {defaultPage == 1 && 
              <p className={fr.cx('fr-pagination__link', 'fr-pagination__link--first')} >
                Première page
              </p>
            }
            {defaultPage > 1 &&
              <Link className={fr.cx('fr-pagination__link', 'fr-pagination__link--first')} href={`${props.href}`}>
              Première page
            </Link>
            }
          </li>
          <li>
            {defaultPage == 1 && 
              <p className={fr.cx('fr-pagination__link', 'fr-pagination__link--prev', 'fr-pagination__link--lg-label')} >
                Page précédente
              </p>
            }
            {defaultPage > 1 &&
              <Link className={fr.cx('fr-pagination__link', 'fr-pagination__link--prev', 'fr-pagination__link--lg-label')} 
                href={`${props.href}/page/${defaultPage-1}`}
              >
                Page précédente
              </Link>
            }
          </li>
          {pages.map(p =>
            <li key={p.number}>
              {p.active && 
                <p className={fr.cx('fr-pagination__link')} aria-current={'page'}>
                  {p.number}
                </p>
              }
              {!p.active &&
                <Link className={fr.cx('fr-pagination__link')} 
                  href={`${props.href}/page/${p.number}`}
                >
                  {p.number}
                </Link>
              }
            </li>   
          )}
          <li>
            {defaultPage == props.count && 
              <p className={fr.cx('fr-pagination__link', 'fr-pagination__link--next', 'fr-pagination__link--lg-label')} >
                Page suivante
              </p>
            }
            {defaultPage != props.count &&
              <Link className={fr.cx('fr-pagination__link', 'fr-pagination__link--next', 'fr-pagination__link--lg-label')} 
                href={`${props.href}/page/${defaultPage+1}`}
              >
                Page suivante
              </Link>
            }
          </li>
          <li>
            {defaultPage == props.count && 
              <p className={fr.cx('fr-pagination__link', 'fr-pagination__link--last')} >
                Dernière page
              </p>
            }
            {defaultPage != props.count &&
              <Link className={fr.cx('fr-pagination__link', 'fr-pagination__link--last')} 
                href={`${props.href}/page/${props.count}`}
              >
                Dernière page
              </Link>
            }
          </li>
        </ul>
      </nav>
      </div>
    </div> 
  );
}