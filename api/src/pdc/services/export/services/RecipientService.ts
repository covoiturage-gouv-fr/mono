import { provider } from "@/ilos/common/index.ts";
import { logger } from "@/lib/logger/index.ts";
import { ExportRecipient } from "../models/ExportRecipient.ts";
import { UserRepository } from "../repositories/UserRepository.ts";

export abstract class RecipientServiceInterfaceResolver {
  /**
   * Add the creator as recipient if no recipient is provided
   *
   * @todo check the evolution of the user's service
   *
   * @param {ExportRecipient[]} _recipients
   * @param {number} _created_by
   * @returns {Promise<ExportRecipient[]>}
   */
  public maybeAddCreator(
    _recipients: ExportRecipient[],
    _created_by: number,
  ): Promise<ExportRecipient[]> {
    throw new Error("Not implemented");
  }
}

@provider({
  identifier: RecipientServiceInterfaceResolver,
})
export class RecipientService {
  constructor(protected userRepository: UserRepository) {}

  public async maybeAddCreator(recipients: ExportRecipient[], created_by: number): Promise<ExportRecipient[]> {
    if (recipients.length) return recipients;

    try {
      const creator = await this.userRepository.find(created_by);
      return creator ? [ExportRecipient.fromEmail(`${creator.fullname} <${creator.email}>`)] : [];
    } catch (e) {
      if (Error.isError(e)) {
        logger.error(`[RecipientService:maybeAddCreator] Error while fetching creator_id ${created_by}: ${e.message}`);
      } else {
        logger.error(`[RecipientService:maybeAddCreator] Error while fetching creator_id ${created_by}:`, e);
      }
      return [];
    }
  }
}
