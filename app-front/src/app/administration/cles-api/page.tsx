"use client"
import { useAuth } from "@/providers/AuthProvider";
import { useState } from "react";
import OperatorCredentialsTable from "../tables/OperatorCredentialsTable";

export default function TabOperatorCredentials() {
  const { user } = useAuth();
  const [key, setKey] = useState(0);
  const refresh = () => {
    setKey((prev) => prev + 1);
  };
  if (!user?.operator_id) return <div>Cette fonctionnalité n&apos;est pas disponible avec vos droits d&apos;accès</div>;
  return (
    <>
      {user?.operator_id && (
        <OperatorCredentialsTable
          key={key}
          title={`Administration des clés de l'API`}
          operatorId={user?.operator_id}
          refresh={refresh}
        />
      )}
    </>
  );
}
