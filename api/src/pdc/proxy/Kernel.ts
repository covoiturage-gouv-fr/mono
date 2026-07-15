import { kernel } from "@/ilos/common/index.ts";
import { RedisConnection } from "@/ilos/connection-redis/index.ts";
import { Kernel as BaseKernel } from "@/ilos/framework/index.ts";
import { SentryProvider } from "@/pdc/providers/sentry/index.ts";
import { ListCommand } from "@/pdc/proxy/commands/ListCommand.ts";
import { AuthServiceProvider } from "@/pdc/services/auth/AuthServiceProvider.ts";
import { MonitoringServiceProvider } from "@/pdc/services/monitoring/MonitoringServiceProvider.ts";
import { DenoPostgresConnection, LegacyPostgresConnection } from "../../ilos/connection-postgres/index.ts";
import { NotificationMailTransporter } from "../providers/notification/NotificationMailTransporter.ts";
import { HandlebarsTemplateProvider } from "../providers/template/HandlebarsTemplateProvider.ts";
import { AcquisitionServiceProvider } from "../services/acquisition/AcquisitionServiceProvider.ts";
import { APDFServiceProvider } from "../services/apdf/APDFServiceProvider.ts";
import { CompanyServiceProvider } from "../services/company/CompanyServiceProvider.ts";
import { DashboardServiceProvider } from "../services/dashboard/DashboardServiceProvider.ts";
import { ExportServiceProvider } from "../services/export/ExportServiceProvider.ts";
import { GeoServiceProvider } from "../services/geo/GeoServiceProvider.ts";
import { HonorServiceProvider } from "../services/honor/HonorServiceProvider.ts";
import { OperatorServiceProvider } from "../services/operator/OperatorServiceProvider.ts";
import { PolicyServiceProvider } from "../services/policy/PolicyServiceProvider.ts";
import { TerritoryServiceProvider } from "../services/territory/TerritoryServiceProvider.ts";
import { config } from "./config/index.ts";

@kernel({
  config,
  children: [
    AuthServiceProvider,
    AcquisitionServiceProvider,
    APDFServiceProvider,
    CompanyServiceProvider,
    ExportServiceProvider,
    HonorServiceProvider,
    MonitoringServiceProvider,
    OperatorServiceProvider,
    PolicyServiceProvider,
    TerritoryServiceProvider,
    DashboardServiceProvider,
    GeoServiceProvider,
  ],
  commands: [ListCommand],
  providers: [
    SentryProvider,
    NotificationMailTransporter,
    HandlebarsTemplateProvider,
    [RedisConnection, new RedisConnection(config.connections.redis)],
    [LegacyPostgresConnection, new LegacyPostgresConnection(config.connections.postgres)],
    [DenoPostgresConnection, new DenoPostgresConnection()],
  ],
})
export class Kernel extends BaseKernel {}
