import { Generator } from './generator_contract';
import * as path from 'path';
import * as fs from 'fs';

/**
 * Loads generator modules from a directory.
 *
 * Each file in the directory (*.js or *.ts) is expected to export an object
 * implementing the Generator interface. Files that fail to load or don't
 * conform are logged and skipped.
 */
export function loadGenerators(generatorsPath: string): Map<string, Generator> {
  const generators = new Map<string, Generator>();

  if (!fs.existsSync(generatorsPath)) {
    console.log(`Generators path does not exist: ${generatorsPath}, no generators loaded`);
    return generators;
  }

  const files = fs.readdirSync(generatorsPath).filter(
    (f) => f.endsWith('.js') || f.endsWith('.ts')
  );

  for (const file of files) {
    const filePath = path.resolve(generatorsPath, file);

    try {
      const module = require(filePath);
      const generator: Generator = module.default || module;

      if (generator && generator.name && typeof generator.generate === 'function') {
        generators.set(generator.name, generator);
        console.log(`Loaded generator: ${generator.name} (from ${file})`);
      } else {
        console.warn(`Skipping ${file}: missing required Generator interface (name, generate)`);
      }
    } catch (error) {
      console.error(`Failed to load generator from ${file}:`, error);
    }
  }

  console.log(`Loaded ${generators.size} generator(s) from ${generatorsPath}`);
  return generators;
}
