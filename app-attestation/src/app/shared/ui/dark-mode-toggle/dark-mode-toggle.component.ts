import { Component, OnInit } from '@angular/core';
import { UntypedFormControl } from '@angular/forms';

type Scheme = 'dark' | 'light';

@Component({
  selector: 'app-dark-mode-toggle',
  templateUrl: './dark-mode-toggle.component.html',
})
export class DarkModeToggleComponent implements OnInit {
  toggle = new UntypedFormControl();

  private scheme: Scheme = 'light';

  constructor() {}

  ngOnInit(): void {
    // fetch the user value from localStorage
    // or fallback to browser preferences. Defaults to light
    const ls = localStorage.getItem('scheme') || this.scheme;

    this.scheme =
      ['dark', 'light'].indexOf(ls) > -1
        ? (ls as Scheme)
        : window.matchMedia('(prefers-color-scheme: dark)').matches
        ? 'dark'
        : 'light';

    this.apply(this.scheme);
    this.toggle.setValue(this.scheme === 'dark', { emitEvent: false });

    this.toggle.valueChanges.subscribe((isDark: boolean) => {
      this.apply(isDark ? 'dark' : 'light');
    });
  }

  private apply(scheme: string) {
    localStorage.setItem('scheme', scheme);
    document.documentElement.setAttribute('data-theme', scheme);
  }
}
