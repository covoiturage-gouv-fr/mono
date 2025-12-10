import { RowsProps } from '@/interfaces/vitrine/componentsInterfaces';
import { fr } from '@codegouvfr/react-dsfr';
import VitrineCard from './VitrineCard';



export default function VitrineRows(props: RowsProps) {
  return (
    <div className={fr.cx('fr-grid-row','fr-grid-row--gutters', 'fr-mt-5w')}>
      {props.data.map((r, i:number) =>
        { 
          switch(r.__component){
            case 'row.title':
              return(
                <div key={i} className={fr.cx('fr-col-12')}>
                  <h2>{r.title}</h2>
                </div>
              )
            case 'row.card':
              return(
                <div className={fr.cx('fr-col-12', 'fr-col-md-6')}>
                  <VitrineCard 
                    key={i} 
                    title={r.title}
                    content={r.content}
                    img={r.img.data.attributes.url}
                    tags={r.tags.data.map((t:any) => t.attributes.label)}
                    buttons={r.buttons}
                  />
                </div>
              )             
          }
        }            
      )}
    </div>
  );
}
