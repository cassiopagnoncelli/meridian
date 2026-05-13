#!/usr/bin/env node
import { join, resolve } from "node:path";
import { performance } from "node:perf_hooks";

import { Meridian } from "../dist/index.js";

const dataDir = resolve(optionValue("--data-dir") ?? join(process.cwd(), "lib", "meridian"));
const iterations = Number(optionValue("--n") ?? process.env.N ?? "10000");
if (!Number.isInteger(iterations) || iterations <= 0) {
  throw new Error(`Invalid benchmark iteration count: ${iterations}`);
}

const openStart = performance.now();
const meridian = await Meridian.open({ dataDir });
const openMs = performance.now() - openStart;

console.log(`open: ${openMs.toFixed(2)}ms`);
bench("ibge", iterations, () => {
  meridian.ibge("São Paulo", "SP");
});
bench("ghsl", iterations, () => {
  meridian.ghsl("São Paulo", "Brazil");
});
bench("ip", iterations, () => {
  meridian.ip("8.8.8.8");
});

function bench(name, iterations, fn) {
  const startedAt = performance.now();
  for (let index = 0; index < iterations; index += 1) {
    fn();
  }
  const elapsedMs = performance.now() - startedAt;
  const perSecond = (iterations / elapsedMs) * 1000;
  console.log(`${name}: ${elapsedMs.toFixed(2)}ms (${perSecond.toFixed(0)} lookups/sec, n=${iterations})`);
}

function optionValue(name) {
  const index = process.argv.indexOf(name);
  if (index === -1) {
    return null;
  }
  const value = process.argv[index + 1];
  return value && !value.startsWith("--") ? value : null;
}
