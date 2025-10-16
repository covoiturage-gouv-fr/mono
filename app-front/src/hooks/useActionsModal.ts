/* eslint-disable @typescript-eslint/restrict-template-expressions */
/* eslint-disable @typescript-eslint/no-unsafe-argument */
/* eslint-disable @typescript-eslint/no-unsafe-assignment */
import { useCallback, useState } from "react";
import { ZodError, type ZodSchema, type ZodType } from "zod";
import { getApiUrl } from "../helpers/api";

export const formatErrors = (
  formattedErrors: Record<string, string[] | undefined>,
): Record<string, string> => {
  return Object.keys(formattedErrors).reduce(
    (acc, key) => {
      acc[key] = formattedErrors[key]?.[0] ?? "";
      return acc;
    },
    {} as Record<string, string>,
  );
};

export const useActionsModal = <T extends Record<string, unknown>>() => {
  const [openModal, setOpenModal] = useState(false);
  const [typeModal, setTypeModal] = useState<"update" | "delete" | "create">(
    "update",
  );
  const [currentRow, setCurrentRow] = useState<T | Record<string, unknown>>({});
  const [errors, setErrors] = useState<Record<string, string> | undefined>({});
  const [submitData, setSubmitData] = useState<T>();
  const [submitError, setSubmitError] = useState<Error>();
  const [submitLoading, setSubmitLoading] = useState<boolean>(true);

  const modalTitle = (type: "update" | "delete" | "create") => {
    switch (type) {
      case "update":
        return "Modifier";
      case "delete":
        return "Supprimer";
      case "create":
        return "Ajouter";
      default:
        return "Action";
    }
  };
  const validateInputChange = <T extends Record<string, unknown>>(
    schema: ZodType<T>,
    field: keyof T,
    value: T[typeof field],
  ) => {
    setCurrentRow((prev) => {
      const updatedRow = { ...prev, [field]: value };
      try {
        schema.parse(updatedRow);
        setErrors({});
      } catch (error) {
        if (error instanceof ZodError) {
          setErrors(formatErrors(error.flatten().fieldErrors));
        }
      }
      return updatedRow;
    });
  };

  const submitModal = useCallback(
    async (url: string, validationSchema: ZodSchema) => {
      try {
        if (typeModal !== "delete") {
          const result = validationSchema.safeParse(currentRow);
          if (!result.success) {
            const errors = result.error.flatten().fieldErrors;
            setErrors(formatErrors(errors));
          }
        } else {
          setErrors({});
        }
        setSubmitLoading(true);
        const request = {
          url: "",
          params: {
            method: "",
            credentials: "include",
            headers: {
              "Content-Type": "application/json",
            },
            body: JSON.stringify(currentRow) as BodyInit,
          } as RequestInit,
        };
        switch (typeModal) {
          case "update":
            request.url = getApiUrl("v3", url);
            request.params.method = "PUT";
            break;
          case "delete":
            request.url = getApiUrl("v3", `${url}/${currentRow.id}`);
            request.params.method = "DELETE";
            request.params.body = undefined;
            break;
          case "create":
            request.url = getApiUrl("v3", url);
            request.params.method = "POST";
            break;
          default:
            break;
        }
        const response = await fetch(request.url, request.params);
        const res = await response.json();
        if (!response.ok) {
          throw new Error(res.message ?? "Une erreur est survenue");
        }
        setSubmitData(res);
        return;
      } catch (e) {
        setSubmitError(e as Error);
        throw e;
      } finally {
        setSubmitLoading(false);
      }
    },
    [currentRow, typeModal],
  );

  return {
    modalTitle,
    currentRow,
    setCurrentRow,
    errors,
    setErrors,
    openModal,
    setOpenModal,
    typeModal,
    setTypeModal,
    validateInputChange,
    submitModal,
    submitLoading,
    submitError,
    submitData,
    setSubmitData,
    setSubmitError,
    setSubmitLoading,
  };
};
