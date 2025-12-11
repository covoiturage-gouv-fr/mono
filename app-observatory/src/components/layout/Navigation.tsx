'use client';
import { MainNavigation } from '@codegouvfr/react-dsfr/MainNavigation';
import { usePathname } from 'next/navigation';

export default function Navigation() {
  const pathname = usePathname();
  return (
    <MainNavigation
      id='header-navigation'
      items={[
        {
          linkProps: { href: '/', 
            target: '_self',
            title: 'Accueil' 
          },
          text: 'Accueil',
          isActive: pathname === '/',
        },
        {
          menuLinks: [
            {
              linkProps: {
                href: '/observatoire/comprendre-covoiturage-quotidien',
                title: 'Comprendre le covoiturage quotidien en France',
                'aria-label': 'Comprendre le covoiturage quotidien en France'
              },
              text: 'Comprendre le covoiturage quotidien en France',
              isActive: pathname === '/observatoire/comprendre-covoiturage-quotidien',
            },
            {
              linkProps: {
                href: '/observatoire/territoire',
                title: 'Comprendre le covoiturage quotidien sur votre territoire',
                'aria-label': 'Comprendre le covoiturage quotidien sur votre territoire',
              },
              text: 'Comprendre le covoiturage quotidien sur votre territoire',
              isActive: pathname.startsWith('/observatoire/territoire'),
            },
            {
              linkProps: {
                href: '/observatoire/campagnes-incitation',
                title: 'Recensement des campagnes d\'incitation',
              },
              text: 'Recensement des campagnes d\'incitation',
              isActive: pathname === '/observatoire/campagnes-incitation',
            },
            {
              linkProps: {
                href: '/observatoire/evaluation-plan-covoiturage',
                title: 'Evaluation du plan national covoiturage',
                'aria-label': 'Evaluation du plan national covoiturage',
              },
              text: 'Evaluation du plan national covoiturage',
              isActive: pathname === '/observatoire/evaluation-plan-covoiturage',
            },
            {
              linkProps: {
                href: '/observatoire/presentation',
                title: 'L\'observatoire en quelques mots',
                'aria-label': 'L\'observatoire en quelques mots',
              },
              text: 'L\'observatoire en quelques mots',
              isActive: pathname === '/observatoire/presentation',
            },
            {
              linkProps: {
                href: '/observatoire/glossaire-des-indicateurs',
              },
              text: 'Glossaire des indicateurs de l\'observatoire',
              isActive: pathname === '/observatoire/glossaire-des-indicateurs',
            },
          ],
          text: 'Observatoire',
          isActive: pathname.startsWith('/observatoire'),
        },
        {
          menuLinks: [
            {
              linkProps: {
                href: '/collectivites/comprendre-et-planifier-le-covoiturage',
                title: 'Planifier et évaluer le covoiturage',
                'aria-label': 'Planifier et évaluer le covoiturage',
              },
              text: 'Comprendre et planifier le covoiturage',
              isActive: pathname === '/collectivites/comprendre-et-planifier-le-covoiturage',
            },
            {
              linkProps: {
                href: '/collectivites/aires-covoiturage',
                title: 'Construire des aires de covoiturage',
                'aria-label': 'Construire des aires de covoiturage',
              },
              text: 'Construire des aires de covoiturage',
              isActive: pathname === '/collectivites/aires-covoiturage',
            },
            {
              linkProps: {
                href: '/collectivites/voies-reservees',
                title: 'Construire des voies réservées au covoiturage',
                'aria-label': 'Construire des voies réservées au covoiturage',
              },
              text: 'Construire des voies réservées au covoiturage',
              isActive: pathname === '/collectivites/voies-reservees',
            },
            {
              linkProps: {
                href: '/collectivites/ligne-de-covoiturage',
                title: 'Construire des lignes de covoiturage',
                'aria-label': 'Construire des lignes de covoiturage',
              },
              text: 'Construire des lignes de covoiturage',
              isActive: pathname === '/collectivites/ligne-de-covoiturage',
            },
            {
              linkProps: {
                href: '/collectivites/auto-stop-organise',
                title: 'Construire un réseau d\'auto-stop organisé',
                'aria-label': 'Construire un réseau d\'auto-stop organisé',
              },
              text: 'Construire un réseau d\'auto-stop organisé',
              isActive: pathname === '/collectivites/auto-stop-organise',
            },
            {
              linkProps: {
                href: '/collectivites/campagnes-incitation-financieres',
                title: 'Déployer une campagne d\'incitations financières',
                'aria-label': 'Déployer une campagne d\'incitations financières',
              },
              text: 'Déployer une campagne d\'incitations financières',
              isActive: pathname === '/collectivites/campagnes-incitation-financieres',
            },
            {
              linkProps: {
                href: '/collectivites/plateformes-numeriques',
                title: 'Déployer une plateforme numérique de covoiturage en marque blanche',
                'aria-label': 'Déployer une plateforme numérique de covoiturage en marque blanche',
              },
              text: 'Déployer une plateforme numérique de covoiturage en marque blanche',
              isActive: pathname === '/collectivites/plateformes-numeriques',
            },
            {
              linkProps: {
                href: '/collectivites/communication-animation',
                title: 'Communiquer et animer son territoire',
                'aria-label': 'Communiquer et animer son territoire',
              },
              text: 'Communiquer et animer son territoire',
              isActive: pathname === '/collectivites/communication-animation',
            },
            {
              linkProps: {
                href: 'https://aides-territoires.beta.gouv.fr/programmes/fonds-vert/',
                title: 'Demander le fond vert',
                'aria-label': 'Demander le fond vert',
              },
              text: 'Demander le fond vert',
            },
          ],
          text: 'Collectivités',
          isActive: pathname.startsWith('/collectivites'),
        },
        {
          menuLinks: [
            {
              linkProps: {
                href: '/autres-acteurs/covoitureurs',
                title: 'Vous êtes covoitureur',
                'aria-label': 'Vous êtes covoitureur',
              },
              text: 'Covoitureurs',
              isActive: pathname === '/autres-acteurs/covoitureurs',
            },
            {
              linkProps: {
                href: '/autres-acteurs/employeurs',
                title: 'Vous êtes un employeur',
                'aria-label': 'Vous êtes un employeur',
              },
              text: 'Employeurs',
              isActive: pathname === '/autres-acteurs/employeurs',
            },
            {
              linkProps: {
                href: '/autres-acteurs/plateformes',
                title: 'Vous êtes une Plateforme de covoiturage',
                'aria-label': 'Vous êtes une Plateforme de covoiturage',
              },
              text: 'Plateformes de covoiturages',
              isActive: pathname === '/autres-acteurs/plateformes',
            }
          ],
          text: 'Autres acteurs',
          isActive: pathname.startsWith('/autres-acteurs'),
        },
        {
          linkProps: { 
            href: '/actualites',
            target: '_self',
            title: 'Actualités',
            'aria-label': 'Actualités'
          },
          text: 'Actualités',
          isActive: pathname.startsWith('/actualites'),
        },
        {
          linkProps: { 
            href: '/ressources',
            target: '_self',
            title: 'Ressources',
            'aria-label': 'Ressources'
          },
          text: 'Ressources',
          isActive: pathname.startsWith('/ressources'),
        },
      ]}
    />
  );
}
