"use client";

import { Input } from "@codegouvfr/react-dsfr/Input";
import { ReactNode, useEffect, useId, useRef, useState } from "react";

export type AutocompleteProps = {
  label: ReactNode | string;
  arialabel: string;
  value: string;
  onChange: (value: string) => void;
  /** async data source — returns the list of suggestion labels for a term */
  search: (term: string) => Promise<string[]>;
  onBlur?: () => void;
  state?: "default" | "error" | "success" | "info";
  stateRelatedMessage?: string;
  /** minimum characters before searching (default 2) */
  minChars?: number;
  /** debounce delay in ms (default 250) */
  debounceMs?: number;
};

/**
 * Generic accessible combobox (WAI-ARIA "combobox with listbox popup" pattern).
 * Domain-agnostic: pass any async `search` function. DSFR-styled via the Input
 * component. Suitable for address, company (SIRENE), or any type-ahead field.
 */
export function Autocomplete({
  label,
  arialabel,
  value,
  onChange,
  search,
  onBlur,
  state = "default",
  stateRelatedMessage,
  minChars = 2,
  debounceMs = 250,
}: AutocompleteProps) {
  const [items, setItems] = useState<string[]>([]);
  const [open, setOpen] = useState(false);
  const [activeIndex, setActiveIndex] = useState(-1);
  const [loading, setLoading] = useState(false);

  const containerRef = useRef<HTMLDivElement>(null);
  const debounceRef = useRef<number>(0);
  // monotonic id so a slow, out-of-order search response can't overwrite a newer one
  const requestRef = useRef<number>(0);

  const listboxId = useId();
  const optionId = (i: number) => `${listboxId}-option-${i}`;

  // close the popup when clicking outside
  useEffect(() => {
    const onDocClick = (e: MouseEvent) => {
      if (!containerRef.current?.contains(e.target as Node)) {
        setOpen(false);
        setActiveIndex(-1);
      }
    };
    document.addEventListener("click", onDocClick);
    return () => document.removeEventListener("click", onDocClick);
  }, []);

  const runSearch = (term: string) => {
    onChange(term);
    setActiveIndex(-1);

    if (term.trim().length < minChars) {
      requestRef.current++;
      setItems([]);
      setOpen(false);
      setLoading(false);
      return;
    }

    setLoading(true);
    window.clearTimeout(debounceRef.current);
    const requestId = ++requestRef.current;
    debounceRef.current = window.setTimeout(async () => {
      const results = await search(term);
      // discard if a newer search has been triggered since
      if (requestId !== requestRef.current) return;
      setItems(results);
      setOpen(results.length > 0);
      setLoading(false);
    }, debounceMs);
  };

  const select = (item: string) => {
    requestRef.current++;
    onChange(item);
    setItems([]);
    setOpen(false);
    setActiveIndex(-1);
    setLoading(false);
  };

  const onKeyDown = (e: React.KeyboardEvent) => {
    switch (e.key) {
      case "ArrowDown":
        e.preventDefault();
        if (open) setActiveIndex((i) => Math.min(i + 1, items.length - 1));
        else if (items.length) setOpen(true);
        break;
      case "ArrowUp":
        e.preventDefault();
        setActiveIndex((i) => Math.max(i - 1, 0));
        break;
      case "Enter":
        // while the popup is open, Enter selects the highlighted option (if any)
        // instead of submitting the surrounding form
        if (open) {
          e.preventDefault();
          if (activeIndex >= 0) select(items[activeIndex]);
        }
        break;
      case "Escape":
        setOpen(false);
        setActiveIndex(-1);
        break;
    }
  };

  return (
    <div ref={containerRef} style={{ position: "relative", marginBottom: "1.5rem" }}>
      <Input
        label={label}
        nativeInputProps={{
          value,
          onChange: (e) => runSearch(e.target.value),
          onKeyDown,
          onBlur,
          autoComplete: "off",
          role: "combobox",
          "aria-expanded": open,
          "aria-controls": open ? listboxId : undefined,
          "aria-autocomplete": "list",
          "aria-activedescendant": activeIndex >= 0 ? optionId(activeIndex) : undefined,
          "aria-busy": loading || undefined,
        }}
        state={state}
        stateRelatedMessage={stateRelatedMessage}
      />

      {open && (
        <ul
          id={listboxId}
          role="listbox"
          aria-label={arialabel}
          style={{
            position: "absolute",
            zIndex: 10,
            width: "100%",
            margin: 0,
            padding: 0,
            listStyle: "none",
            background: "var(--background-overlap-grey)",
            boxShadow: "var(--overlap-shadow, 0 2px 6px rgba(0,0,0,.16))",
            maxHeight: "15rem",
            overflowY: "auto",
          }}
        >
          {items.map((item, i) => (
            <li
              key={`${item}-${i}`}
              id={optionId(i)}
              role="option"
              aria-selected={i === activeIndex}
              // onMouseDown (not onClick) + preventDefault so the input's
              // onBlur doesn't fire and steal the selection before it lands
              onMouseDown={(e) => {
                e.preventDefault();
                select(item);
              }}
              onMouseEnter={() => setActiveIndex(i)}
              style={{
                padding: "0.5rem 1rem",
                cursor: "pointer",
                background: i === activeIndex ? "var(--background-contrast-grey)" : "transparent",
              }}
            >
              {item}
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
