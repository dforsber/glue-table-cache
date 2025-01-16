// eslint.config.js
const typescript = require("@typescript-eslint/eslint-plugin");
const typescriptParser = require("@typescript-eslint/parser");
const jest = require("eslint-plugin-jest");

module.exports = [
  {
    ignores: [
      "node_modules/**",
      "dist/**", 
      "coverage/**",
      ".git/**"
    ]
  },
  {
    files: ["src/**/*.ts", "test/**/*.ts"],
    plugins: {
      "@typescript-eslint": typescript,
      jest: jest
    },
    languageOptions: {
      parser: typescriptParser,
      ecmaVersion: 2020,
      sourceType: "module"
    },
    rules: {
      // TypeScript
      "@typescript-eslint/no-explicit-any": "warn",
      "@typescript-eslint/no-unused-vars": [
        "error",
        {
          argsIgnorePattern: "^_",
          varsIgnorePattern: "^_"
        }
      ],

      // General
      "no-console": ["warn", { allow: ["warn", "error"] }],
      "no-debugger": "error",
      "no-duplicate-case": "error", 
      "no-empty": ["error", { allowEmptyCatch: true }],
      "no-extra-semi": "error",
      "no-trailing-spaces": "error",

      // Best practices
      "eqeqeq": ["error", "always", { null: "ignore" }],
      "no-var": "error",
      "prefer-const": "error",

      // Jest
      "jest/no-disabled-tests": "warn",
      "jest/no-focused-tests": "error",
      "jest/valid-expect": "error",
      "jest/expect-expect": "error"
    }
  }
];
