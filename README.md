# ETL Assignment

## Setup
1. Install [Node.js](https://nodejs.org/) and [PostgreSQL](https://www.postgresql.org/download/).
2. Create a database named `etl`.
3. Run the SQL script:
```bash
psql -U postgres -d etl -f db.sql
```

## Install dependencies
```bash
npm install
```

## Run the server
```bash
npm run start-server
```

## Send events to server
```bash
npm run run-client
```

## Process events and update DB
You can process any event file by passing the filename:
```bash
node data_processor.js events.jsonl
node data_processor.js another_events.jsonl

## Query revenue for a user
```bash
curl http://localhost:8000/userEvents/user1