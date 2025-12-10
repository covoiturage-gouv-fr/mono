import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';
import { FormPublicComponent } from './pages/form-public/form-public.component';
import { FormLimitedComponent } from './pages/form-limited/form-limited.component';
import { FormEmployerComponent } from './pages/form-employer/form-employer.component';

const routes: Routes = [
  { path: '', pathMatch: 'prefix', redirectTo: 'salarie-secteur-prive' },
  {
    path: 'salarie-secteur-public',
    component: FormPublicComponent,
    data: { title: 'Salarié du secteur public' },
  },
  {
    path: 'salarie-secteur-prive',
    component: FormLimitedComponent,
    data: { title: 'Salarié du secteur privé' },
  },
  {
    path: 'je-suis-employeur',
    component: FormEmployerComponent,
    data: { title: 'Employeur' },
  },
];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule],
})
export class AppRoutingModule {}
