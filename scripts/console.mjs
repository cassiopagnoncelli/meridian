#!/usr/bin/env node
import repl from "node:repl";
import { inspect } from "node:util";
import { join, resolve } from "node:path";

import { Meridian } from "../dist/index.js";

const dataDir = resolve(optionValue("--data-dir") ?? process.env.MERIDIAN_DATA_DIR ?? join(process.cwd(), "lib", "meridian"));
const sources = parseSources(optionValue("--sources"));
const strict = !hasFlag("--no-strict");

const meridian = await Meridian.open({
  dataDir,
  strict,
  ...(sources ? { sources } : {})
});

const helpers = {
  meridian,
  m: meridian,
  ip: (ipAddress) => meridian.ip(ipAddress),
  ibge: (city, state) => meridian.ibge(city, state),
  ghsl: (city, country) => meridian.ghsl(city, country),
  sources: () => meridian.sources(),
  metadata: () => meridian.metadata(),
  close: () => meridian.close()
};

const evalCode = optionValue("--eval") ?? optionValue("-e");
if (evalCode) {
  try {
    const result = await evaluate(evalCode, helpers);
    if (result !== undefined) {
      console.log(format(result));
    }
  } finally {
    meridian.close();
  }
} else {
  startConsole();
}

function startConsole() {
  console.log(`Meridian console loaded from ${dataDir}`);
  console.log('Helpers: ip("8.8.8.8"), ibge("São Paulo", "SP"), ghsl("São Paulo", "Brazil"), sources(), metadata(), meridian');

  const server = repl.start({
    prompt: "meridian> ",
    useColors: process.stdout.isTTY,
    ignoreUndefined: true,
    writer: format
  });

  Object.assign(server.context, helpers);
  server.defineCommand("meridian", {
    help: "Show Meridian console helpers",
    action() {
      this.clearBufferedCommand();
      console.log('Helpers: ip("8.8.8.8"), ibge("São Paulo", "SP"), ghsl("São Paulo", "Brazil"), sources(), metadata(), meridian');
      this.displayPrompt();
    }
  });
  server.on("exit", () => {
    meridian.close();
  });
}

function format(value) {
  return inspect(value, {
    colors: process.stdout.isTTY,
    compact: false,
    depth: null,
    maxArrayLength: 50,
    breakLength: 100
  });
}

async function evaluate(code, context) {
  const names = Object.keys(context);
  const values = Object.values(context);
  const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor;

  try {
    return await new AsyncFunction(...names, `return (${code});`)(...values);
  } catch (error) {
    if (!(error instanceof SyntaxError)) {
      throw error;
    }
    return await new AsyncFunction(...names, code)(...values);
  }
}

function parseSources(value) {
  if (!value) {
    return null;
  }

  const sources = value.split(",").map((source) => source.trim()).filter(Boolean);
  const allowed = new Set(["maxmind", "ibge", "ghsl"]);
  const invalid = sources.filter((source) => !allowed.has(source));
  if (invalid.length > 0) {
    throw new Error(`Invalid source(s): ${invalid.join(", ")}`);
  }
  return sources;
}

function optionValue(name) {
  const index = process.argv.indexOf(name);
  if (index === -1) return null;
  const value = process.argv[index + 1];
  return value && !value.startsWith("--") ? value : null;
}

function hasFlag(name) {
  return process.argv.includes(name);
}
