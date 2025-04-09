const fs = require('fs');
const readline = require('readline');
const axios = require('axios');

const EVENTS_FILE = 'events.jsonl';
const ENDPOINT = 'http://localhost:8000/liveEvent';

const sendEvents = async () => {
    const fileStream = fs.createReadStream(EVENTS_FILE);
    const rl = readline.createInterface({ input: fileStream, crlfDelay: Infinity });

    for await (const line of rl) {
        try {
            const event = JSON.parse(line);
            await axios.post(ENDPOINT, event, { headers: { Authorization: 'secret' } });
            console.log(`Sent event: ${event}`);
        } catch (error) {
            console.error(`Failed to send event: ${error.message}`);
        }
    }
}

sendEvents();