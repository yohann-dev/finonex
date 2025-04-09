const fs = require('fs');
const readline = require('readline');
const { Pool } = require('pg');

const fileName = process.argv[2];
if (!fileName) {
  console.error('Please provide a filename to process. Example: node data_processor.js events.jsonl');
  process.exit(1);
}

const pool = new Pool({
    user: 'postgres',
    host: 'localhost',
    database: 'etl',
    password: 'postgres',
    port: 5432
});

const getDelta = (name, value) => {
    if (name === 'add_revenue') {
        return value;
    } else if (name === 'subtract_revenue') {
        return -value;
    }
    return 0;
}

const getMapFromFile = async (filePath) => {
    const revenueMap = new Map();

    const fileStream = fs.createReadStream(filePath);
    const rl = readline.createInterface({ input: fileStream, crlfDelay: Infinity });

    for await (const line of rl) {
        try {
            const { userId, name, value } = JSON.parse(line);
            const delta = getDelta(name, value);

            revenueMap.set(userId, (revenueMap.get(userId) || 0) + delta);
        } catch (error) {
            console.error(`Invalid JSON line: ${error.message}`);
        }
    }

    console.log('Finished reading events file.');

    return revenueMap;
}

const saveRevenuesInDB = async (revenuesMap) => {
    for (const [userId, revenue] of revenuesMap.entries()) {
        await pool.query(
            `INSERT INTO users_revenue (user_id, revenue)
             VALUES ($1, $2)
             ON CONFLICT (user_id) DO UPDATE SET revenue = users_revenue.revenue + $2`,
            [userId, revenue]
        );
    }
    console.log('Finished processing events.');
}

const processEvents = async (fileName) => {
    const revenueMap = await getMapFromFile(fileName);

    await saveRevenuesInDB(revenueMap);

    process.exit();
}

processEvents(fileName);