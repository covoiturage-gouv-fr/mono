import { PublicForm } from "@/components/forms/PublicForm";
import { StartDsfrOnHydration } from "@/dsfr-bootstrap";

export default function SalarieSecteurPublic() {
  return (
    <div>
      <StartDsfrOnHydration />
      <PublicForm />
    </div>
  );
}
