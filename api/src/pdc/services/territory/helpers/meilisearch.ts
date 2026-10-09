import { getPerformanceTimer, logger } from "@/lib/logger/index.ts";
import { type Config, Meilisearch, type RecordAny } from "dep:meilisearch";

export async function indexData<T extends RecordAny>(
  config: Config,
  indexName: string,
  batchSize: number,
  documents: T[],
) {
  try {
    const msg = `Données indexées avec succès dans MeiliSearch`;
    const timer = getPerformanceTimer();

    const client = new Meilisearch(config);

    // Selection de l'index. Un index est créé s'il n'existe pas
    const index = client.index(indexName);

    // On supprime les documents de l'index
    await index.deleteAllDocuments();

    // Indexation des données dans MeiliSearch
    await Promise.all(index.addDocumentsInBatches(documents, batchSize));

    logger.info(`${msg} in ${timer.stop()} ms`);
  } catch (e) {
    logger.error(`Erreur lors de l\'indexation des données: ${e.message}`);
  }
}
