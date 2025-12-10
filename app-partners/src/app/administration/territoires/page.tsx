"use client"; 
import { useAuth } from "@/providers/AuthProvider";
import TerritoriesTable from "../tables/TerritoriesTable";

export default function TabTerritories() {
  const { user } = useAuth();
  return (
    <>
      <TerritoriesTable title={`Gestion des territoires`} id={user?.territory_id ?? null} />
    </>
  );
}
