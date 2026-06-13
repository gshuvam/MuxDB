import { defineConfig } from "tsup";

export default defineConfig({
  entry: {
    index: "src/index.ts",
  },
  format: ["cjs", "esm"],
  dts: true,
  splitting: true,
  sourcemap: true,
  clean: true,
  treeshake: true,
  target: "node18",
  outDir: "dist",
  external: [
    "pg",
    "@prisma/client",
    "drizzle-orm",
    "knex",
    "typeorm",
    "sequelize",
    "@mikro-orm/core",
    "mongoose",
    "ioredis",
    "kysely",
    "reflect-metadata",
  ],
});
