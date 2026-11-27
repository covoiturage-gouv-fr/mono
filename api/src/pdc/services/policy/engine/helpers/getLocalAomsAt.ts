/**
 * Fin de campagne d'une AOM locale : à partir de `date`, elle est réintégrée
 * (ses trajets internes redeviennent éligibles à la campagne de la Région).
 * Écrire la date avec un décalage explicite (heure de Paris).
 */
export type LocalAomEnds = Array<{ siren: string; date: Date }>;

/** Liste des AOM locales encore exclues à la date donnée (toutes si pas de date) */
export function getLocalAomsAt<T extends string>(all: readonly T[], ends: LocalAomEnds, current?: Date): T[] {
  if (!current) return [...all];
  const reintegrated = new Set(
    ends.filter(({ date }) => current.getTime() >= date.getTime()).map(({ siren }) => siren),
  );
  return all.filter((siren) => !reintegrated.has(siren));
}
