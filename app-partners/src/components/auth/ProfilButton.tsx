"use client";
import { activeScopeLabel, labelRole } from "@/helpers/auth";
import { useAuth } from "@/providers/AuthProvider";
import Button from "@codegouvfr/react-dsfr/Button";
import Tag from "@codegouvfr/react-dsfr/Tag";

export function ProfilButton() {
  const { isAuth, user } = useAuth();
  const scopeLabel = activeScopeLabel(user);

  return (
    <>
      {isAuth && (
        <>
          <Button
            priority="primary"
            linkProps={{
              href: "/administration",
            }}
          >
            <div style={{ display: "block" }}>
              <div>{user?.name}</div>
              <div>{labelRole(user?.role ?? "")}</div>
              {scopeLabel && <Tag>{scopeLabel}</Tag>}
            </div>
          </Button>
        </>
      )}
    </>
  );
}
