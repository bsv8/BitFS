/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        brand: {
          50: "#eef8ff",
          100: "#d8eeff",
          500: "#0f6cad",
          700: "#0b4d7f",
          900: "#072b48"
        }
      }
    }
  },
  plugins: []
};
