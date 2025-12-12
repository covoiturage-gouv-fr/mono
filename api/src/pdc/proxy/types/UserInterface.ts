export interface UserInterface {
  email: string;
  firstname: string;
  lastname: string;
  phone?: string;
  group?: string; // computed prop
  role: string;
  operator_id?: number;
  territory_id?: number;
  _id: number;
  permissions: string[];
  status: string;
  created_at: Date;
  updated_at: Date;
}
