'use client'
import { ShareProps } from '@/interfaces/common/componentsInterface';
import { FrCxArg, fr } from '@codegouvfr/react-dsfr';

export default function Share(props:{social:ShareProps[], location: string}) {
  const handleClick = (e:React.MouseEvent<HTMLAnchorElement, MouseEvent>, url:string, title:string) => {
    e.preventDefault();
    window.open(url,title,'toolbar=no,location=yes,status=no,menubar=no,scrollbars=yes,resizable=yes,width=600,height=450'); 
  };

  return (
    <div className={fr.cx('fr-share')}>
      <p className={fr.cx('fr-share__title')}>Partager la page</p>
      <ul className={fr.cx('fr-share__group')}>
        {props.social && props.social.map((s, i) => 
          <li key={i}>
            <a className={fr.cx('fr-share__link',s.icon as FrCxArg)} 
              title={`Partager sur ${s.name} | nouvelle fenêtre`} 
              aria-label={`Partager sur ${s.name} | nouvelle fenêtre`} 
              href={s.href} 
              target="_blank" 
              rel="noopener" 
              onClick={(e) => handleClick(e, s.href, `Partager sur ${s.name}`)}
            >
              Partager sur {s.name}
            </a>
          </li>
        )}
        <li>
          <button className={fr.cx('fr-share__link', 'fr-share__link--copy')}
            title="Copier dans le presse-papier" 
            onClick={() => {navigator.clipboard.writeText(props.location);
            alert('Adresse copiée dans le presse papier.');}}>
            Copier dans le presse-papier
          </button>
        </li>
      </ul>
    </div>
  );
}
