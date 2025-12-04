import { ExtensionInterface, NewableType, serviceProvider } from "../../../ilos/common/index.ts";
import { ServiceProvider as AbstractServiceProvider } from "../../../ilos/core/index.ts";
import { defaultMiddlewareBindings } from "../../providers/middleware/index.ts";
import { defaultNotificationBindings } from "../../providers/notification/index.ts";
import { ValidatorExtension, ValidatorMiddleware } from "../../providers/validator/index.ts";
import { ChangePasswordUserAction } from "./actions/ChangePasswordUserAction.ts";
import { ChangePasswordWithTokenUserAction } from "./actions/ChangePasswordWithTokenUserAction.ts";
import { ChangeRoleUserAction } from "./actions/ChangeRoleUserAction.ts";
import { CheckForgottenTokenUserAction } from "./actions/CheckForgottenTokenUserAction.ts";
import { ConfirmEmailUserAction } from "./actions/ConfirmEmailUserAction.ts";
import { ContactformAction } from "./actions/ContactformAction.ts";
import { CreateUserAction } from "./actions/CreateUserAction.ts";
import { DeleteAssociatedUserAction } from "./actions/DeleteAssociatedUserAction.ts";
import { DeleteUserAction } from "./actions/DeleteUserAction.ts";
import { FindUserAction } from "./actions/FindUserAction.ts";
import { ForgottenPasswordUserAction } from "./actions/ForgottenPasswordUserAction.ts";
import { HasUsersAction } from "./actions/HasUsersAction.ts";
import { ListUserAction } from "./actions/ListUserAction.ts";
import { LoginUserAction } from "./actions/LoginUserAction.ts";
import { NotifyUserAction } from "./actions/NotifyUserAction.ts";
import { PatchUserAction } from "./actions/PatchUserAction.ts";
import { SendConfirmEmailUserAction } from "./actions/SendConfirmEmailUserAction.ts";
import { SendInvitationEmailUserAction } from "./actions/SendInvitationEmailUserAction.ts";
import { SimulatePolicyformAction } from "./actions/SimulatePolicyformAction.ts";
import { FindInactiveCommand } from "./commands/FindInactiveCommand.ts";
import { SeedUsersCommand } from "./commands/SeedUsersCommand.ts";
import { config } from "./config/index.ts";
import { changePassword } from "./contracts/changePassword.schema.ts";
import { changePasswordWithToken } from "./contracts/changePasswordWithToken.schema.ts";
import { changeRole } from "./contracts/changeRole.schema.ts";
import { checkForgottenToken } from "./contracts/checkForgottenToken.schema.ts";
import { confirmEmail } from "./contracts/confirmEmail.schema.ts";
import { contactform } from "./contracts/contactform.schema.ts";
import { create } from "./contracts/create.schema.ts";
import { deleteUser } from "./contracts/delete.schema.ts";
import { deleteAssociatedUser } from "./contracts/deleteAssociated.schema.ts";
import { find } from "./contracts/find.schema.ts";
import { forgottenPassword } from "./contracts/forgottenPassword.schema.ts";
import { list } from "./contracts/list.schema.ts";
import { login } from "./contracts/login.schema.ts";
import { patch } from "./contracts/patch.schema.ts";
import { sendConfirmEmail } from "./contracts/sendConfirmEmail.schema.ts";
import { sendInvitationEmail } from "./contracts/sendInvitationEmail.schema.ts";
import { sendSimulationEmail } from "./contracts/simulatePolicyform.schema.ts";
import { challengePasswordMiddlewareBinding } from "./middlewares/ChallengePasswordMiddleware.ts";
import { challengeTokenMiddlewareBinding } from "./middlewares/ChallengeTokenMiddleware.ts";
import { AuthRepositoryProvider } from "./providers/AuthRepositoryProvider.ts";
import { UserNotificationProvider } from "./providers/UserNotificationProvider.ts";
import { UserPgRepositoryProvider } from "./providers/UserPgRepositoryProvider.ts";

@serviceProvider({
  config,
  providers: [
    ...defaultNotificationBindings,
    UserPgRepositoryProvider,
    AuthRepositoryProvider,
    UserNotificationProvider,
  ],
  validator: [
    ["user.changePassword", changePassword],
    ["user.changePasswordWithToken", changePasswordWithToken],
    ["user.changeRole", changeRole],
    ["user.checkForgottenToken", checkForgottenToken],
    ["user.confirmEmail", confirmEmail],
    ["user.contactform", contactform],
    ["user.create", create],
    ["user.delete", deleteUser],
    ["user.deleteAssociated", deleteAssociatedUser],
    ["user.find", find],
    ["user.forgottenPassword", forgottenPassword],
    ["user.list", list],
    ["user.login", login],
    ["user.patch", patch],
    ["user.sendConfirmEmail", sendConfirmEmail],
    ["user.sendInvitationEmail", sendInvitationEmail],
    ["user.sendSimulationEmail", sendSimulationEmail],
  ],
  middlewares: [
    ...defaultMiddlewareBindings,
    challengePasswordMiddlewareBinding,
    challengeTokenMiddlewareBinding,
    ["validate", ValidatorMiddleware],
  ],
  handlers: [
    ChangePasswordUserAction,
    ChangePasswordWithTokenUserAction,
    ChangeRoleUserAction,
    CheckForgottenTokenUserAction,
    ConfirmEmailUserAction,
    ContactformAction,
    CreateUserAction,
    DeleteUserAction,
    DeleteAssociatedUserAction,
    FindUserAction,
    ForgottenPasswordUserAction,
    ListUserAction,
    LoginUserAction,
    NotifyUserAction,
    PatchUserAction,
    SendConfirmEmailUserAction,
    SendInvitationEmailUserAction,
    HasUsersAction,
    SimulatePolicyformAction,
  ],
  commands: [SeedUsersCommand, FindInactiveCommand],
})
export class UserServiceProvider extends AbstractServiceProvider {
  override readonly extensions: NewableType<ExtensionInterface>[] = [ValidatorExtension];
}
