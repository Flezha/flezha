// CommonJS half of the runtime shim. See src/runtime-shim.d.ts.
'use strict';

exports.runtimeDir = __dirname;

exports.requireNative = function requireNative(specifier) {
  return require(specifier);
};
