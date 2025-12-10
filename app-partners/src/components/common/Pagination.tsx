import { type PaginationProps } from "@/interfaces/componentsInterface";
import { fr } from "@codegouvfr/react-dsfr";
import Link from "next/link";

export default function Pagination(props: PaginationProps) {
  const getPagination = (count: number, defaultPage: number) => {
    const maxVisiblePages = 10;
    const slicesSize = 4;
    // first n pages
    if (count <= maxVisiblePages) {
      return Array.from({ length: count }, (_, v) => ({
        number: v + 1,
        active: defaultPage === v + 1,
      }));
    }
    // last n pages
    if (defaultPage > count - maxVisiblePages) {
      return Array.from({ length: maxVisiblePages }, (_, v) => {
        const pageNumber = count - (maxVisiblePages - v) + 1;
        return {
          number: pageNumber,
          active: defaultPage === pageNumber,
        };
      });
    }
    // slices
    return [
      ...Array.from({ length: slicesSize }, (_, v) => {
        if (defaultPage > slicesSize) {
          const pageNumber = v + defaultPage;
          return { number: pageNumber, active: defaultPage === pageNumber };
        }
        return { number: v + 1, active: defaultPage === v + 1 };
      }),
      ...Array.from({ length: slicesSize }, (_, v) => {
        const pageNumber = count - (slicesSize - v) + 1;
        return {
          number: pageNumber,
          active: defaultPage === pageNumber,
        };
      }),
    ];
  };
  const defaultPage = props.defaultPage ?? 1;
  const pages = getPagination(props.count, defaultPage);

  return (
    <div className={fr.cx("fr-grid-row", "fr-mt-5w")}>
      <div className={fr.cx("fr-mx-auto")}>
        <nav role="navigation" className={fr.cx("fr-pagination")} aria-label="Pagination">
          <ul className={fr.cx("fr-pagination__list")}>
            <li>
              {defaultPage == 1 && (
                <p className={fr.cx("fr-pagination__link", "fr-pagination__link--first")}>Première page</p>
              )}
              {defaultPage > 1 && (
                <Link
                  className={fr.cx("fr-pagination__link", "fr-pagination__link--first")}
                  href=""
                  onClick={() => props.onChange(1)}
                >
                  Première page
                </Link>
              )}
            </li>
            <li>
              {defaultPage == 1 && (
                <p
                  className={fr.cx("fr-pagination__link", "fr-pagination__link--prev", "fr-pagination__link--lg-label")}
                >
                  Page précédente
                </p>
              )}
              {defaultPage > 1 && (
                <Link
                  className={fr.cx("fr-pagination__link", "fr-pagination__link--prev", "fr-pagination__link--lg-label")}
                  href=""
                  onClick={() => props.onChange(defaultPage - 1)}
                >
                  Page précédente
                </Link>
              )}
            </li>
            {pages.map((p) => (
              <li key={p.number}>
                {p.active && (
                  <p className={fr.cx("fr-pagination__link")} aria-current={"page"}>
                    {p.number}
                  </p>
                )}
                {!p.active && (
                  <Link className={fr.cx("fr-pagination__link")} href="" onClick={() => props.onChange(p.number)}>
                    {p.number}
                  </Link>
                )}
              </li>
            ))}
            <li>
              {defaultPage == props.count && (
                <p
                  className={fr.cx("fr-pagination__link", "fr-pagination__link--next", "fr-pagination__link--lg-label")}
                >
                  Page suivante
                </p>
              )}
              {defaultPage != props.count && (
                <Link
                  className={fr.cx("fr-pagination__link", "fr-pagination__link--next", "fr-pagination__link--lg-label")}
                  href=""
                  onClick={() => props.onChange(defaultPage + 1)}
                >
                  Page suivante
                </Link>
              )}
            </li>
            <li>
              {defaultPage == props.count && (
                <p className={fr.cx("fr-pagination__link", "fr-pagination__link--last")}>Dernière page</p>
              )}
              {defaultPage != props.count && (
                <Link
                  className={fr.cx("fr-pagination__link", "fr-pagination__link--last")}
                  href=""
                  onClick={() => props.onChange(props.count)}
                >
                  Dernière page
                </Link>
              )}
            </li>
          </ul>
        </nav>
      </div>
    </div>
  );
}
