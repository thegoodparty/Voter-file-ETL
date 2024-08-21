import fs from "fs";
import { join } from "path";
import { readFileSync } from "fs";
import { getDMMF } from "@prisma/sdk";
import unzipper from "unzipper";
import { exec } from "child_process";
import { promisify } from "util";
import request from "request-promise";

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

export function fileExists(localFiles: string[], remoteFile: string) {
  // check the remote .zip file against the local .tab files.
  const remoteFileParts = remoteFile.split(".");
  for (let localFile of localFiles) {
    const localFileParts = localFile.split(".");
    if (localFileParts[0] === remoteFileParts[0]) {
      return true;
    }
  }
  return false;
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

export async function countLines(filename: string): Promise<number> {
  const execAsync = promisify(exec);
  try {
    const { stdout, stderr } = await execAsync(`wc -l ${filename}`);
    if (stderr) {
      console.error(`stderr: ${stderr}`);
      return 0;
    }
    const lines = parseInt(stdout.trim());
    console.log(`Number of lines in ${filename}: ${lines}`);
    return lines;
  } catch (error) {
    console.error(`Error executing wc -l: ${error}`);
  }
  return 0;
}

export async function getModelFields(modelName: string) {
  // get the field names and types for the model from the prisma schema.
  const modelPath = join(__dirname, `prisma/schema/${modelName}.prisma`);
  const modelSchema = readFileSync(modelPath, "utf-8");
  const prismaPath = join(__dirname, `prisma/schema/schema.prisma`);
  const prismaSchema = readFileSync(prismaPath, "utf-8");
  const schema = prismaSchema + "\n\n" + modelSchema;
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

export async function sendSlackMessage(message: string): Promise<void> {
  try {
    const slackAppId = process.env.SLACK_APP_ID;
    const token = process.env.SLACK_DEV_CHANNEL_TOKEN;
    const slackChannelId = process.env.SLACK_DEV_CHANNEL_ID;

    if (!slackChannelId || !slackAppId || !token) {
      throw new Error("Missing Env Variables");
    }

    let formattedMessage = {
      blocks: [
        {
          type: "section",
          text: {
            type: "mrkdwn",
            text: message,
          },
        },
      ],
    };

    const options = {
      uri: `https://hooks.slack.com/services/${slackAppId}/${slackChannelId}/${token}`,
      method: "POST",
      json: true,
      body: formattedMessage,
    };

    await request(options);
    console.log("Slack message sent successfully");
  } catch (e) {
    console.error("Error sending Slack message", e);
  }
}
