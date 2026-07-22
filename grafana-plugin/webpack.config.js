const path = require('path');

module.exports = {
  mode: 'production',
  target: 'web',
  entry: './src/module.ts',
  output: {
    path: path.resolve(__dirname, 'dist'),
    filename: 'module.js',
    libraryTarget: 'amd',
    publicPath: '/',
    clean: false,
  },
  resolve: {
    extensions: ['.ts', '.tsx', '.js'],
  },
  module: {
    rules: [
      {
        test: /\.tsx?$/,
        use: {
          loader: 'ts-loader',
          options: {
            compilerOptions: {
              noEmit: false,
            },
          },
        },
        exclude: /node_modules/,
      },
    ],
  },
  externals: [
    'react',
    'react-dom',
    'react-dom/client',
    '@grafana/data',
    '@grafana/ui',
    '@grafana/runtime',
    '@grafana/schema',
    /^@emotion\/.*/,
    'lodash',
    'moment',
    'rxjs',
  ],
  devtool: 'source-map',
};
