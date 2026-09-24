"use client";
import { useAuth } from "@/providers/AuthProvider";
import OperatorCredentialsTable from "../tables/OperatorCredentialsTable";

export default function TabOperatorCredentials() {
  const { user } = useAuth();
  if (!user?.operator_id) return <div>Cette fonctionnalité n&apos;est pas disponible avec vos droits d&apos;accès</div>;
  return <OperatorCredentialsTable title={`Administration des clés de l'API`} operatorId={user.operator_id} />;
}
