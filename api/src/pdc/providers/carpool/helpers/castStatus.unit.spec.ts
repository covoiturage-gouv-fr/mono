import { assertEquals } from "dep:assert";
import { it } from "dep:testing-bdd";
import { CarpoolAnomalyStatusEnum } from "@/pdc/providers/carpool/interfaces/common.ts";
import { CarpoolAcquisitionStatusEnum, CarpoolFraudStatusEnum, CarpoolStatusEnum } from "../interfaces/index.ts";
import { castFromStatusEnum, castToStatusEnum } from "./castStatus.ts";

for (
  const { title, args, expected } of [
    {
      title: "Pending fraud status + received -> pending",
      args: {
        acquisition_status: CarpoolAcquisitionStatusEnum.Received,
        fraud_status: CarpoolFraudStatusEnum.Pending,
      },
      expected: CarpoolStatusEnum.Pending,
    },
    {
      title: "Pending fraud status + updated -> pending",
      args: {
        acquisition_status: CarpoolAcquisitionStatusEnum.Updated,
        fraud_status: CarpoolFraudStatusEnum.Pending,
      },
      expected: CarpoolStatusEnum.Pending,
    },
    {
      title: "Pending fraud status + processed -> ok",
      args: {
        acquisition_status: CarpoolAcquisitionStatusEnum.Processed,
        fraud_status: CarpoolFraudStatusEnum.Pending,
      },
      expected: CarpoolStatusEnum.Pending,
    },
    {
      title: "Pending fraud status + failed -> acquisition_error",
      args: {
        acquisition_status: CarpoolAcquisitionStatusEnum.Failed,
      },
      expected: CarpoolStatusEnum.AcquisitionError,
    },
    {
      title: "Pending fraud status + canceled -> canceled",
      args: {
        acquisition_status: CarpoolAcquisitionStatusEnum.Canceled,
        fraud_status: CarpoolFraudStatusEnum.Pending,
      },
      expected: CarpoolStatusEnum.Canceled,
    },
    {
      title: "Pending fraud status + expired -> term_violation_error",
      args: {
        acquisition_status: CarpoolAcquisitionStatusEnum.Expired,
        fraud_status: CarpoolFraudStatusEnum.Pending,
      },
      expected: CarpoolStatusEnum.TermsViolationError,
    },
    {
      title: "Passed fraud status + received -> ok",
      args: {
        acquisition_status: CarpoolAcquisitionStatusEnum.Processed,
        fraud_status: CarpoolFraudStatusEnum.Passed,
        anomaly_status: CarpoolAnomalyStatusEnum.Passed,
      },
      expected: CarpoolStatusEnum.Ok,
    },
    {
      title: "Passed fraud status + updated -> ok",
      args: {
        acquisition_status: CarpoolAcquisitionStatusEnum.Updated,
        fraud_status: CarpoolFraudStatusEnum.Passed,
      },
      expected: CarpoolStatusEnum.Pending,
    },
    {
      title: "Passed fraud status + failed -> anomaly_error",
      args: {
        acquisition_status: CarpoolAcquisitionStatusEnum.Processed,
        anomaly_status: CarpoolAnomalyStatusEnum.Failed,
      },
      expected: CarpoolStatusEnum.AnomalyError,
    },
    {
      title: "Passed fraud status + canceled -> canceled",
      args: {
        acquisition_status: CarpoolAcquisitionStatusEnum.Canceled,
        fraud_status: CarpoolFraudStatusEnum.Passed,
      },
      expected: CarpoolStatusEnum.Canceled,
    },
    {
      title: "Passed fraud status + expired -> term_violation_error",
      args: {
        acquisition_status: CarpoolAcquisitionStatusEnum.Expired,
        fraud_status: CarpoolFraudStatusEnum.Passed,
      },
      expected: CarpoolStatusEnum.TermsViolationError,
    },
    {
      title: "Failed fraud status + processed -> fraudcheck_error",
      args: {
        acquisition_status: CarpoolAcquisitionStatusEnum.Processed,
        fraud_status: CarpoolFraudStatusEnum.Failed,
      },
      expected: CarpoolStatusEnum.FraudError,
    },
    {
      title: "Failed anomaly status + Expired -> term_violation_error",
      args: {
        acquisition_status: CarpoolAcquisitionStatusEnum.Expired,
        anomaly_status: CarpoolAnomalyStatusEnum.Failed,
      },
      expected: CarpoolStatusEnum.TermsViolationError,
    },
    {
      title: "Passed fraud without anomaly -> pending",
      args: {
        acquisition_status: CarpoolAcquisitionStatusEnum.Processed,
        fraud_status: CarpoolFraudStatusEnum.Passed,
        // anomaly_status: CarpoolAnomalyStatusEnum.Passed,
      },
      expected: CarpoolStatusEnum.Pending,
    },
    {
      title: "Passed fraud + anomaly -> ok",
      args: {
        acquisition_status: CarpoolAcquisitionStatusEnum.Processed,
        fraud_status: CarpoolFraudStatusEnum.Passed,
        anomaly_status: CarpoolAnomalyStatusEnum.Passed,
      },
      expected: CarpoolStatusEnum.Ok,
    },
  ]
) {
  it(title, () => {
    assertEquals(castToStatusEnum(args), expected);
  });
}

for (
  const { title, args, expected } of [
    {
      title: "Pending",
      expected: [
        { acquisition_status: CarpoolAcquisitionStatusEnum.Received },
        { acquisition_status: CarpoolAcquisitionStatusEnum.Updated },
        { fraud_status: CarpoolFraudStatusEnum.Pending },
        { anomaly_status: CarpoolAnomalyStatusEnum.Pending },
      ],
      args: CarpoolStatusEnum.Pending,
    },
    {
      title: "Ok",
      expected: [
        {
          acquisition_status: CarpoolAcquisitionStatusEnum.Processed,
          fraud_status: CarpoolFraudStatusEnum.Passed,
          anomaly_status: CarpoolAnomalyStatusEnum.Passed,
        },
      ],
      args: CarpoolStatusEnum.Ok,
    },
    {
      title: "Fraud",
      expected: [
        {
          fraud_status: CarpoolFraudStatusEnum.Failed,
        },
      ],
      args: CarpoolStatusEnum.FraudError,
    },
  ]
) {
  it(title, () => {
    assertEquals(castFromStatusEnum(args), expected);
  });
}
