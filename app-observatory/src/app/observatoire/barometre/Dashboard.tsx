'use client';
import { barometerPointsUrl, filterPoints, providerColors, providersOf } from '@/helpers/barometer';
import { useApi } from '@/hooks/useApi';
import type { BarometerPoint } from '@/interfaces/observatoire/barometer';
import { fr } from '@codegouvfr/react-dsfr';
import { Select } from '@codegouvfr/react-dsfr/Select';
import { usePathname, useRouter, useSearchParams } from 'next/navigation';
import { useCallback, useMemo } from 'react';
import BarometerMap from './BarometerMap';
import PointDetails from './PointDetails';

const pointsUrl = barometerPointsUrl();

export default function Dashboard() {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const providerId = searchParams.get('fournisseur');
  const pointId = searchParams.get('point');

  const { data, error, loading } = useApi<BarometerPoint[]>(pointsUrl);
  const points = useMemo(() => data ?? [], [data]);
  const providers = useMemo(() => providersOf(points), [points]);
  const colors = useMemo(() => providerColors(providers), [providers]);
  const visible = useMemo(() => filterPoints(points, providerId), [points, providerId]);
  const selected = points.find((p) => String(p.id) === pointId);

  const navigate = useCallback(
    (next: { fournisseur?: string | null; point?: string | null }) => {
      const params = new URLSearchParams(searchParams.toString());
      for (const [key, value] of Object.entries(next)) {
        if (value) params.set(key, value);
        else params.delete(key);
      }
      router.replace(`${pathname}?${params}`, { scroll: false });
    },
    [router, pathname, searchParams],
  );

  const onProviderChange = (id: string) => {
    // Le point sélectionné n'appartient plus forcément au fournisseur choisi.
    const keepPoint = selected && (!id || selected.fournisseur_id === id);
    navigate({ fournisseur: id || null, point: keepPoint ? String(selected.id) : null });
  };

  const filters = (
    <>
      <div className={fr.cx('fr-h6')}>Filtres de recherche</div>
      <Select
        label='Fournisseur de données'
        nativeSelectProps={{ value: providerId ?? '', onChange: (e) => onProviderChange(e.target.value) }}
      >
        <option value=''>Tous les fournisseurs</option>
        {providers.map((p) => (
          <option key={p.id} value={p.id}>{p.name}</option>
        ))}
      </Select>
      <Select
        label='Point de captage'
        nativeSelectProps={{ value: selected?.id ?? '', onChange: (e) => navigate({ point: e.target.value || null }) }}
      >
        <option value='' disabled>Sélectionner un point</option>
        {visible.map((p) => (
          <option key={p.id} value={p.id}>{p.nom}</option>
        ))}
      </Select>
      <p className={fr.cx('fr-text--sm')}>
        {visible.length} point{visible.length > 1 ? 's' : ''} de captage
      </p>
    </>
  );

  return (
    <>
      <h2 className={fr.cx('fr-h4', 'fr-mt-5w')}>Sélectionner un point de captage</h2>
      <BarometerMap
        points={visible}
        providers={providers}
        colors={colors}
        selectedId={selected?.id ?? null}
        loading={loading}
        error={error}
        sidebar={filters}
        onSelect={(id) => navigate({ point: String(id) })}
      />
      {selected ? (
        <PointDetails point={selected} />
      ) : (
        !loading && !error && (
          <p className={fr.cx('fr-mt-3w')}>
            Sélectionnez un point de captage sur la carte ou dans les filtres pour afficher ses résultats.
          </p>
        )
      )}
    </>
  );
}
