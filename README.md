# Goodparty.org Voterfiles ETL client

Loads voter files from s3 or local files into postgres using prisma.

# Installation:

Install dependencies:

```
npm install
```

Make sure your postgres user has sufficient permissions (for development only):

```
CREATE USER username WITH PASSWORD 'password';
GRANT ALL ON SCHEMA public TO username;
CREATE DATABASE "dbname" OWNER username;
GRANT ALL PRIVILEGES ON DATABASE "dbname" TO username;
ALTER ROLE username CREATEDB;
```

You must reload configuration on the postgres server after.

Copy .env file and update with your DATABASE_URL and other variables.

```
cp .env.example .env
```

Generate the schema:

```
npx prisma generate
```

Run migrations:

```
npx prisma migrate dev
```

Run the downloader:

```
npm run download
```

Run the loader:

```
npm run load
```

Run the s3 loader:

```
npm run load-s3
```
