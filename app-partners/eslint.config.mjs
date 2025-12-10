import eslint from "@eslint/js";
import prettierConfig from "eslint-config-prettier";
import { defineConfig, globalIgnores } from "eslint/config";
import tseslint from "typescript-eslint";

export default defineConfig(
  eslint.configs.recommended,
  tseslint.configs.recommendedTypeChecked,
  tseslint.configs.stylisticTypeChecked,
  prettierConfig,
  globalIgnores([
    "**/node_modules/**",
    ".next/**",
    "out/**",
    "dist/**",
    "build/**",
    "coverage/**",
    "*.config.mjs",
    "*.config.js",
    "next-env.d.ts",
  ]),
  {
    languageOptions: {
      parserOptions: {
        projectService: true,
        tsconfigRootDir: import.meta.dirname,
      },
    },
    rules: {
      "@typescript-eslint/require-await": "warn",
      "@typescript-eslint/no-unsafe-member-access": "warn",
      "@typescript-eslint/no-unsafe-argument": "warn",
      "@typescript-eslint/no-unsafe-assignment": "warn",
      "no-case-declarations": "warn",
    },
  },
);
