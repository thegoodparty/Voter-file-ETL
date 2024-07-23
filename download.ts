import Client from "ssh2-sftp-client";
import path from "path";
import unzipper from "unzipper";

const sftp = new Client();

const remoteDir = "/VM2Uniform";
const localDir = "/home/ec2-user/VM2Uniform";

async function downloadAndUnZipFiles() {
  try {
    await sftp.connect({
      host: process.env.SFTP_HOST,
      port: parseInt(process.env?.SFTP_PORT ? process.env.SFTP_PORT : "22"),
      username: process.env.SFTP_USERNAME,
      password: process.env.SFTP_PASSWORD,
    });
    const files = await sftp.list(remoteDir);
    const zipFiles = files.filter((file) => file.name.endsWith(".zip"));

    for (const file of zipFiles) {
      const remoteFilePath = path.join(remoteDir, file.name);
      const localFilePath = path.join(localDir, file.name);
      console.log(`Downloading ${file.name} to ${localFilePath} ...`);
      // Download file
      await sftp.get(remoteFilePath, localFilePath);
      console.log(`${file.name} has been downloaded successfully.`);
      // Unzip the file
      console.log(`Unzipping ${file.name} ...`);
      await unzipFile(localFilePath, localDir);
      console.log(`Unzipped ${file.name}`);

      // TODO: if an older file is detected, delete it.
      // Also, delete each .zip file after we finish.
    }
    console.log("All .zip files have been processed successfully.");
  } catch (err) {
    console.error("Error:", err);
  } finally {
    await sftp.end();
  }
}

async function unzipFile(zipFilePath: string, outputDir: string) {
  try {
    const directory = await unzipper.Open.file(zipFilePath);
    await directory.extract({ path: outputDir, concurrency: 5 });
    console.log(`Unzipped: ${zipFilePath}`);
  } catch (err) {
    console.error("Error unzipping file:", zipFilePath, err);
  }
}

downloadAndUnZipFiles();
