import { Component, OnInit } from '@angular/core';
import {
  UntypedFormGroup,
  UntypedFormControl,
  Validators,
  UntypedFormArray,
  AbstractControl,
  FormControl,
} from '@angular/forms';
import { debounceTime, distinctUntilChanged } from 'rxjs/operators';
import { CounterService } from '../../services/counter.service';
import { AddressService } from '../../services/address.service';
import { CompanyService } from '../../services/company.service';
import { PdfPublicGeneratorService } from '../../services/pdfPublic.service';

@Component({
  selector: 'app-form-public',
  templateUrl: './form-public.component.html',
  styleUrls: ['./form-public.component.scss'],
})
export class FormPublicComponent implements OnInit {
  private readonly CHECKBOXES_COUNT = 8;

  // configure the form fields
  currentYear: number = new Date().getFullYear();
  previousYear: number = new Date().getFullYear() - 1;

  profileForm = new UntypedFormGroup({
    name: new UntypedFormControl('', [Validators.required, Validators.maxLength(51)]),
    ministry: new UntypedFormControl('', [Validators.required, Validators.maxLength(120)]),
    workshare: new UntypedFormControl(null, [
      Validators.required,
      Validators.maxLength(3),
      Validators.pattern(/^[0-9]{0,3}$/),
    ]),
    rank: new UntypedFormControl('', [Validators.required, Validators.maxLength(51)]),
    year: new UntypedFormControl(this.currentYear, [Validators.required]),
    mobility: new UntypedFormControl('no', [Validators.required]),
    mobility_date: new UntypedFormControl(''),
    days: new UntypedFormControl('', [Validators.max(365), Validators.pattern(/^[0-9]{0,6}$/)]),
    home_address: new UntypedFormControl('', [Validators.required, Validators.maxLength(256)]),
    work_address: new UntypedFormControl('', [Validators.required, Validators.maxLength(256)]),
    chk: new UntypedFormArray(
      [],
      [
        Validators.required,
        // validate array length
        (c: AbstractControl<string[]>): { [key: string]: any } | null =>
          c.value.filter((i) => !!i && i !== '').length === this.CHECKBOXES_COUNT ? null : { arrayLength: true },
      ],
    ),
    location: new UntypedFormControl('', [Validators.required, Validators.maxLength(128)]),
  });

  constructor(
    protected addressService: AddressService,
    protected companyService: CompanyService,
    private pdf: PdfPublicGeneratorService,
    private counter: CounterService,
  ) {}

  ngOnInit(): void {
    // reload saved data in a crash free way
    try {
      const saved = localStorage.getItem('formPublic');
      if (saved) {
        const obj = JSON.parse(saved);
        this.profileForm.patchValue(obj);
        obj.chk.forEach((id: string) => {
          if (!id) return;
          (this.profileForm.get('chk') as UntypedFormArray).push(new UntypedFormControl(id));
          // @ts-ignore
          document.getElementById(id).checked = true;
        });
      } else {
        document.querySelectorAll('[id*="checkbox_"]').forEach((box) => {
          if (!box) return;
          // @ts-ignore
          box.checked = true;
          (this.profileForm.get('chk') as UntypedFormArray).push(new UntypedFormControl(box.getAttribute('id')));
        });

        // save to localStorage
        localStorage.setItem('formPublic', JSON.stringify(this.profileForm.value));
      }
    } catch (e) {
      localStorage.removeItem('formPublic');
    }

    // auto-save
    this.profileForm.valueChanges.pipe(debounceTime(250), distinctUntilChanged()).subscribe((o: object) => {
      localStorage.setItem('formPublic', JSON.stringify(o));
    });

    // set mobility_date required stated
    this.profileForm.get('mobility')?.valueChanges.subscribe((value) => this.setMobilityValidators(value));

    // init validators for existing value
    this.setMobilityValidators(this.profileForm.get('mobility')?.value);
  }

  showError(fieldName: string, errorName: string) {
    return this.profileForm.get(fieldName)?.dirty && this.profileForm.get(fieldName)?.hasError(errorName);
  }

  onChkChange(event: Event): void {
    const fa = this.profileForm.get('chk') as UntypedFormArray;
    const target = event.target as HTMLInputElement | null;
    if (!target) return;

    if (target.checked) {
      fa.push(new FormControl<string>(target.value));
    } else {
      let idx = 0;
      fa.controls.forEach((ctrl: AbstractControl<string>) => {
        if (ctrl.value === target.value) {
          fa.removeAt(idx);
          return;
        }
        idx += 1;
      });
    }
  }

  onFound(key: string, value: string): void {
    this.profileForm.get(key)?.setValue(value);
  }

  onReset(): void {
    localStorage.removeItem('formPublic');
  }

  async onSubmit() {
    this.pdf.generate(this.profileForm.value, {
      onComplete: () => alert('Attestation générée'),
    });
    this.counter.save(window.origin, 'public', this.profileForm.get('employer')?.value || '');
  }

  trackByFn(index: number, item: { id: number }) {
    return item.id;
  }

  private setMobilityValidators(value: string): void {
    if (value === 'no') {
      this.profileForm.get('mobility_date')?.clearValidators();
    } else {
      this.profileForm.get('mobility_date')?.setValidators(Validators.required);
    }
    this.profileForm.get('mobility_date')?.updateValueAndValidity();
  }
}
