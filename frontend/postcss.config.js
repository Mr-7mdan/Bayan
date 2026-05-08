module.exports = {
  plugins: {
    // Inline `@import './path.css'` directives into globals.css at build
    // time — required so the alpha theme variant CSS files (loaded from
    // `src/app/themes/`) end up in the same cascade as the rest of the
    // styles. Without this, Next.js's css-loader treats them as separate
    // chunks and the variant selectors never match.
    'postcss-import': {},
    tailwindcss: {},
    autoprefixer: {},
  },
}
