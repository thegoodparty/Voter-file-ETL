# Goodparty.org Voterfiles ETL client

Loads voter files from remote sftp into postgres using prisma.

# Instructions:

Run the downloader:

```
npm run download
```

When the downloader is complete:
Run the loader:

```
npm run load
```

The downloads are tracked in the VoterFile model and their counts are verified after loading. Only newer files are loaded and appended to. Zip files are deleted and older files are also removed automatically. Any failed downloads are reported to slack.

If you need to make any changes to the schema, you simply update the `Voter.prisma` schema
and then Run the app to copy the schema to the state schemas:

```
npm run copy
```

Remember that changes to the schema will require you to run migrate and generate (see below).

# Installation:

Install dependencies:

```
npm install
```

Copy .env file and update with your DATABASE_URL and other variables.

```
cp .env.example .env
```

# Database Setup

If you choose to test with a local database.
Make sure your postgres user has sufficient permissions (for development only):

```
CREATE USER username WITH PASSWORD 'password';
GRANT ALL ON SCHEMA public TO username;
CREATE DATABASE "dbname" OWNER username;
GRANT ALL PRIVILEGES ON DATABASE "dbname" TO username;
ALTER ROLE username CREATEDB;
```

You must reload configuration on the postgres server after.

# Development:

If you alter the schema you must run migrations.

To Run migrations:

```
npx prisma migrate dev
```

Generate the schema:

```
npx prisma generate
```
