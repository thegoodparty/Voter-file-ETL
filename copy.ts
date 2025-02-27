import { readFileSync, writeFileSync } from "fs";
import { join } from "path";

async function main() {
  const states = [
    "AL",
    "AK",
    "AZ",
    "AR",
    "CA",
    "CO",
    "CT",
    "DE",
    "DC",
    "FL",
    "GA",
    "HI",
    "ID",
    "IL",
    "IN",
    "IA",
    "KS",
    "KY",
    "LA",
    "ME",
    "MD",
    "MA",
    "MI",
    "MN",
    "MS",
    "MO",
    "MT",
    "NE",
    "NV",
    "NH",
    "NJ",
    "NM",
    "NY",
    "NC",
    "ND",
    "OH",
    "OK",
    "OR",
    "PA",
    "RI",
    "SC",
    "SD",
    "TN",
    "TX",
    "UT",
    "VT",
    "VA",
    "WA",
    "WV",
    "WI",
    "WY",
  ];

  // open the contents of prisma/Voter.prisma
  // and copy it for each state to prisma/schema/Voter${state}.prisma
  for (const state of states) {
    const templatePath = join(__dirname, `prisma/Voter.prisma`);
    let template = readFileSync(templatePath, "utf-8");
    // let templateModel = template.replace(/model Voter/g, `model Voter${state}`);

    // every state also must have a temporary table Voter${state}Temp
    let templateTemp = template.replace(
      /model Voter/g,
      `model Voter${state}Temp`
    );
    // const combinedTemplate = templateModel + "\n\n" + templateTemp;

    const schemaPath = join(
      __dirname,
      `prisma/schema/Voter${state}Temp.prisma`
    );
    // writeFileSync(schemaPath, combinedTemplate);
    writeFileSync(schemaPath, templateTemp);
  }
}

main();
