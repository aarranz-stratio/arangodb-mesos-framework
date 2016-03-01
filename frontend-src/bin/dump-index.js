#!/usr/bin/env node
require('../server.babel'); // babel registration (runtime transpilation for node)

var path = require('path');
var rootDir = path.resolve(__dirname, '..');

global.__CLIENT__ = true;
global.__SERVER__ = false;
global.__DISABLE_SSR__ = true;  // <----- DISABLES SERVER SIDE RENDERING FOR ERROR DEBUGGING
global.__DEVELOPMENT__ = 'production';
global.__DEVTOOLS__ = false;

var WebpackIsomorphicTools = require('webpack-isomorphic-tools');
global.webpackIsomorphicTools = new WebpackIsomorphicTools(require('../webpack/webpack-isomorphic-tools'))

require('../src/dump-index.js');
