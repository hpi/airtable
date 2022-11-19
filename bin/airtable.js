#!/usr/bin/node env

const fetch = require("node-fetch");
const { Command } = require("commander");
const fs = require("fs/promises");
const dayjs = require("dayjs");
const { resolve } = require("path");

const program = new Command();

process.on("unhandledRejection", onfatal);
process.on("uncaughtException", onfatal);

function onfatal(err) {
  console.log("fatal:", err.message);
  exit(1);
}

function exit(code) {
  process.nextTick(process.exit, code);
}

program
  .command("dump")
  .description("Dump to file")
  .option("-k, --apiKey [apiKey]", "OAuth access token")
  .option(
    "--export-format <format>",
    "Export file format",
    "{date}-airtable.json"
  )
  .option("--export-path [path]", "Export file path")
  .action(dump);

program.parseAsync(process.argv);

async function dump({ apiKey, exportPath, exportFormat }) {
  const filledExportFormat = exportFormat.replace(
    "{date}",
    dayjs().format("YYYY-MM-DD")
  );

  const EXPORT_PATH = resolve(exportPath, filledExportFormat);

  try {
    const getBasesRes = await fetch(`https://api.airtable.com/v0/meta/bases`, {
      method: `GET`,
      headers: {
        Authorization: `Bearer ${apiKey}`,
      },
    });

    const { bases } = await getBasesRes.json();

    const basePromises = bases.map(async (base) => {
      const getTablesRes = await fetch(
        `https://api.airtable.com/v0/meta/bases/${base.id}/tables`,
        {
          method: `GET`,
          headers: {
            Authorization: `Bearer ${apiKey}`,
          },
        }
      );

      const { tables } = await getTablesRes.json();

      const recordPromises = tables.map(async (table) => {
        const getRecordsRes = await fetch(
          `https://api.airtable.com/v0/${base.id}/${table.id}`,
          {
            method: `GET`,
            headers: {
              Authorization: `Bearer ${apiKey}`,
            },
          }
        );

        const { records } = await getRecordsRes.json();

        return {
          name: table.name,
          records,
          description: table.description,
          id: table.id,
        };
      });

      return {
        [base.name]: {
          recordsByTable: await Promise.all(recordPromises),
          id: base.id,
        },
      };
    });

    const recordsByBase = await Promise.all(basePromises);

    const dump = JSON.stringify(Object.assign(...recordsByBase));

    await fs.writeFile(EXPORT_PATH, dump);
  } catch (e) {
    onfatal(e);
  }
}
