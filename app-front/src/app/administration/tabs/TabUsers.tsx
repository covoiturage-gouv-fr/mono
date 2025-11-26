import { useAuth } from "@/providers/AuthProvider";
import UsersTable from "../tables/UsersTable";

export default function TabUsers() {
  const { user } = useAuth();

  return (
    <>
      <UsersTable
        title={`Gestion des utilisateurs`}
        territoryId={user?.territory_id ?? null}
        operatorId={user?.operator_id ?? null}
      />
    </>
  );
}
