import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { HttpClientModule } from '@angular/common/http';
import { BrowserModule } from '@angular/platform-browser';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { ReactiveFormsModule } from '@angular/forms';

import { AppRoutingModule } from './app-routing.module';
import { AppComponent } from './app.component';
import { FormLimitedComponent } from './pages/form-limited/form-limited.component';
import { CompanyService } from './services/company.service';
import { AddressService } from './services/address.service';
import { PdfLimitedGeneratorService } from './services/pdfLimited.service';
import { AutocompleteFormComponent } from './shared/form/autocomplete/autocomplete.component';
import { DarkModeToggleComponent } from './shared/ui/dark-mode-toggle/dark-mode-toggle.component';
import { FormPickatypeComponent } from './pages/form-pickatype/form-pickatype.component';
import { FormPublicComponent } from './pages/form-public/form-public.component';
import { FormEmployerComponent } from './pages/form-employer/form-employer.component';

@NgModule({
  declarations: [
    AppComponent,
    FormLimitedComponent,
    AutocompleteFormComponent,
    DarkModeToggleComponent,
    FormPickatypeComponent,
    FormPublicComponent,
    FormEmployerComponent,
  ],
  imports: [
    CommonModule,
    AppRoutingModule,
    NoopAnimationsModule,
    BrowserModule,
    HttpClientModule,
    ReactiveFormsModule,
  ],
  providers: [CompanyService, AddressService, PdfLimitedGeneratorService],
  bootstrap: [AppComponent],
})
export class AppModule {}
