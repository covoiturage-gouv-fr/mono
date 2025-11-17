"use client";
import { labelRole } from "@/helpers/auth";
import { useAuth } from "@/providers/AuthProvider";
import Button from "@codegouvfr/react-dsfr/Button";
import Tag from "@codegouvfr/react-dsfr/Tag";

export function ProfilButton() {
  const { isAuth, user } = useAuth();

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
              {user?.territory_id && <Tag>Territoire : {user.territory_id}</Tag>}
              {user?.operator_id && <Tag>Opérateur : {user.operator_id}</Tag>}
            </div>
          </Button>
        </>
      )}
    </>
  );
}
