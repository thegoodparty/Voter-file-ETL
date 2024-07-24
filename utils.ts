import fs from "fs";
import { join } from "path";
import { readFileSync } from "fs";
import { getDMMF } from "@prisma/sdk";
import unzipper from "unzipper";

export async function getLocalFiles(localDir: string) {
  let files = [];
  try {
    // get files from the local directory
    files = fs.readdirSync(localDir).filter((file: string) => {
      return file.includes(".tab");
    });

    // order files by filename
    files.sort((a, b) => {
      const aNum = parseInt(a.split("--")[0]);
      const bNum = parseInt(b.split("--")[0]);
      return aNum - bNum;
    });
  } catch (error) {
    console.error("Error fetching files: ", error);
    throw error;
  }
  return files;
}

export async function unzipFile(zipFilePath: string, outputDir: string) {
  try {
    const directory = await unzipper.Open.file(zipFilePath);
    await directory.extract({ path: outputDir, concurrency: 5 });
    console.log(`Unzipped: ${zipFilePath}`);
  } catch (err) {
    console.error("Error unzipping file:", zipFilePath, err);
  }
}

export async function getModelFields(modelName: string) {
  // get the field names and types for the model from the prisma schema.
  const schemaPath = join(__dirname, "prisma/schema.prisma");
  const schema = readFileSync(schemaPath, "utf-8");
  const dmmf = await getDMMF({ datamodel: schema });

  const models = dmmf.datamodel.models;
  let modelFields: { [modelName: string]: string[] } = {};
  let fieldTypes: { [modelName: string]: { [fieldName: string]: string } } = {};

  models.forEach((model) => {
    if (model.name !== modelName) return;
    modelFields[model.name] = model.fields.map((field) => field.name);
    fieldTypes[model.name] = model.fields.reduce((acc: any, field) => {
      acc[field.name] = field.type;
      return acc;
    }, {});
  });

  return { modelFields, fieldTypes };
}
