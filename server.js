const express = require('express');
const fs = require('fs');
const path = require('path');

const app = express();
const port = 8000;
const { Pool } = require('pg');
const SECRET_HEADER = 'secret';
const EVENTS_FILE = path.join(__dirname, 'events.jsonl');

app.use(express.json());

const pool = new Pool({
    user: 'postgres',
    host: 'localhost',
    database: 'etl',
    password: 'postgres',
    port: 5432
});
  
const writeStream = fs.createWriteStream(EVENTS_FILE, { flags: 'a' });

const headerMiddleware = (req, res, next) => {
    const authorization = req.headers['authorization'];
    if (!authorization || authorization !== SECRET_HEADER) {
        return res.status(401).json({ error: 'Unauthorized' });
    }

    next();
};

app.post('/liveEvent', headerMiddleware, (req, res) => {
    const event = JSON.stringify(req.body);

    writeStream.write(event + '\n', err => {
        if (err) {
            console.error('Error writing to file:', err);
            return res.status(500).send('Server error');
        }
        console.log(`Event written to file: ${event}`);
    });

    res.status(200).send('Event received');
})

app.get('/userEvents/:userId', async (req, res) => {
    const { userId } = req.params;

    try {
        const result = await pool.query('SELECT * FROM users_revenue WHERE user_id = $1', [userId]);
        if (result.rows.length === 0) {
            return res.status(404).json({ error: 'User not found' });
        }

        res.json(result.rows[0]);
    } catch (err) {
        res.status(500).json({ error: 'Database error' });
    }
})

app.listen(port, () => console.log(`Server app running on port ${port}`));;