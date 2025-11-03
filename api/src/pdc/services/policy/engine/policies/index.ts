import { PolicyHandlerStaticInterface } from "../../interfaces/index.ts";
import { PaysDeLaLoire2021 } from "./20210105_PaysDeLaLoire.ts";
import { IDFMPeriodeNormale2021 } from "./20210520_IDFM.ts";
import { NantesMetropoleXPCovoitan2021 } from "./20211202_NantesMetropoleXP.ts";
import { LavalAgglo2022 } from "./20220412_LavalAgglo.ts";
import { SMT2022 } from "./20220414_SMT.ts";
import { MetropoleRouenNormandie2022 } from "./20220420_MetropoleRouenNormandie.ts";
import { Occitanie20232024 } from "./20221024_Occitanie.ts";
import { PMGF2022 } from "./20221102_PMGF.ts";
import { Montpellier } from "./20221206_Montpellier.ts";
import { Cotentin2023 } from "./20230101_Cotentin.ts";
import { LaRochelle20232024 } from "./20230101_LaRochelle.ts";
import { Vitre2023 } from "./20230101_Vitre.ts";
import { MetropoleSavoie } from "./20230124_MetropoleSavoie.ts";
import { SMT2023 } from "./20230126_SMT.ts";
import { PaysDeLaLoire2023 } from "./20230201_PaysDeLaLoire.ts";
import { PaysBasque20232024 } from "./20230401_PaysBasqueAdour.ts";
import { Lannion202305 } from "./20230501_Lannion.ts";
import { ATMB202305 } from "./20230502_ATMB.ts";
import { PMGF2023 } from "./20230502_PMGF.ts";
import { GrandPoitiers } from "./20230927_GrandPoitiers.ts";
import { PMGFOctobre2023 } from "./20231001_PMGF.ts";
import { Cannes2024 } from "./20240101_Cannes.ts";
import { LaRochelle2024 } from "./20240101_LaRochelle.ts";
import { NantesMetropole2024 } from "./20240101_NantesMetropole.ts";
import { PaysDeLaLoire2024 } from "./20240101_PaysDeLaLoire.ts";
import { SMTC2024Passenger } from "./20240101_SMTC2024Passenger.ts";
import { SMTC2024Driver } from "./20240101_SMTCDriver.ts";
import { PetrLunevilloisS12023 } from "./20240108_PetrLunevillois.ts";
import { GrandChatellerault2024 } from "./20240201_GrandChatellerault.ts";
import { TerresTouloises2024 } from "./20240201_TerresTouloises.ts";
import { LannionTregor2024 } from "./20240401_LannionTregor.ts";
import { PMGFxATMB2024 } from "./20240401_PMGFxATMB.ts";
import { CCVMM202405 } from "./20240805_CCVMM_2024_05.ts";
import { SiouleLimagne } from "./20240901_SiouleLimagne_2024_09.ts";
import { HauteCorreze2025 } from "./20240902_haute_correze_2025.ts";
import { PetrLunevillois092024032025 } from "./20240902_PetrLunevillois.ts";
import { CCPOA202410 } from "./20241015_CCPOA_orthe_et_arrigans_2024.ts";
import { GrandChatellerault2025 } from "./20250101_GrandChatellerault.ts";
import { GrandPoitiers2025 } from "./20250101_GrandPoitiers2025.ts";
import { LaRochelle2025 } from "./20250101_LaRochelle.ts";
import { PaysBasque2025 } from "./20250101_PaysBasqueAdour.ts";
import { PMGFxATMB2025 } from "./20250101_PMGFxATMB.ts";
import { Rennes2025 } from "./20250101_Rennes.ts";
import { PMGFxATMBxSM4CCx2025 } from "./20250701_PMGFxATMBxSM4CC.ts";
import { HauteCorreze2026 } from "./20250902_haute_correze_2026.ts";
import { PolicyTemplateOne } from "./unbound/PolicyTemplateOne.ts";
import { PolicyTemplateThree } from "./unbound/PolicyTemplateThree.ts";
import { PolicyTemplateTwo } from "./unbound/PolicyTemplateTwo.ts";

export const policies: Map<string, PolicyHandlerStaticInterface> = new Map(
  [
    ATMB202305,
    Cannes2024,
    CCPOA202410,
    CCVMM202405,
    Cotentin2023,
    GrandChatellerault2024,
    GrandChatellerault2025,
    GrandPoitiers,
    GrandPoitiers2025,
    HauteCorreze2025,
    HauteCorreze2026,
    IDFMPeriodeNormale2021,
    Lannion202305,
    LannionTregor2024,
    LaRochelle20232024,
    LaRochelle2024,
    LaRochelle2025,
    LavalAgglo2022,
    MetropoleRouenNormandie2022,
    MetropoleSavoie,
    Montpellier,
    NantesMetropole2024,
    NantesMetropoleXPCovoitan2021,
    Occitanie20232024,
    PaysBasque20232024,
    PaysBasque2025,
    PaysDeLaLoire2021,
    PaysDeLaLoire2023,
    PaysDeLaLoire2024,
    PetrLunevillois092024032025,
    PetrLunevilloisS12023,
    PMGF2022,
    PMGF2023,
    PMGFOctobre2023,
    PMGFxATMB2024,
    PMGFxATMB2025,
    PMGFxATMBxSM4CCx2025,
    PolicyTemplateOne,
    PolicyTemplateThree,
    PolicyTemplateTwo,
    Rennes2025,
    SiouleLimagne,
    SMT2022,
    SMT2023,
    SMTC2024Driver,
    SMTC2024Passenger,
    TerresTouloises2024,
    Vitre2023,
  ].map((h) => [h.id, h]),
);
