const express = require('express');
const mysql = require('mysql');
const moment = require('moment-timezone');
const Device = require('./deviceManager');
const { startNewFetchLogger, getCurrentLogger } = require('./loggerManager');

const app = express();
const PORT = 3000;
const dbConfig = {
    host: process.env.DB_HOST,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
};

const db = mysql.createPool(dbConfig);
const insertQuery = `
	INSERT INTO meter_readings (meter_id, timestamp_start, interval_length, consumption) 
	VALUES ?;
`;
const updateQuery = `
    UPDATE meters
    SET latest_fetch = CURRENT_TIMESTAMP
    WHERE id = ?;
`;

let devices = [];
let nextFetchTimeout;
let logger;

app.use(express.json());
app.use(express.static('public')); // Verzeichnis für statische Dateien (Frontend)

// API Route zum Starten des Abrufprozesses
app.post('/start-fetch', async (req, res) => {
    if (nextFetchTimeout) {
        res.json({ status: 'started' })
        return res.status(400);
    }

    await initializeDevices();
    scheduleNextFetch();
    res.json({ status: 'started' });
});

// API Route zum Stoppen des Abrufprozesses
app.post('/stop-fetch', async (req, res) => {
    if (!nextFetchTimeout) {
        res.json({ status: 'stopped' })
        return res.status(400);
    }
	await processFetchCycle();
    clearTimeout(nextFetchTimeout);
    nextFetchTimeout = null;
    res.json({ status: 'stopped' });
});

// API Route für Status-Updates
app.get('/status', async (req, res) => {
    const statuses = await Promise.all(devices.map(device => device.getStatus()));
    res.json({devices: statuses, fetching: !!nextFetchTimeout });
});

// API Route für Device Refresh
app.get('/refresh', async (req, res) => {
    db.query('SELECT * FROM meters WHERE active = 1', (error, results) => {
        if (error) {
            logger.error('Error fetching devices from meters table');
            return res.status(400).json({ msg: 'DB connection error'});
        }
        
        results.forEach((result) => {
            const deviceIndex = devices.findIndex(device => device.name === result.name);
            if (deviceIndex !== -1) {
                const device = devices[deviceIndex];
                if (device.address !== result.address) {
                    device.address = result.address;
                    device.attemptStartInterval();
                }
            } else {
                const newDevice = new Device({ ...result, logger: logger });
                devices.push(newDevice);
                newDevice.attemptStartInterval();
            }
        });

        res.status(200).json({ msg: 'Devices refreshed successfully'});
    });
});

async function initializeDevices() {
	logger = startNewFetchLogger();
	logger.info('Running Query to initialize Devices...')
    db.query('SELECT * FROM meters WHERE active = 1', (error, results) => {
        if (error) {logger.error('Error fetchin devices from meters table'); throw error;}
        devices = results.map(data => new Device({ ...data, logger: logger }));
		devices.forEach(device => device.attemptStartInterval())
    });
}

function scheduleNextFetch() {
    const now = new Date();
    const secondsToNextHalfHour = (60 - now.getSeconds()) % 60;
    const minutesToNextHalfHour = 15 - now.getMinutes() % 15 - (secondsToNextHalfHour === 0 ? 0 : 1);
    const msToNextFetch = minutesToNextHalfHour * 60000 + secondsToNextHalfHour * 1000 - now.getMilliseconds();
	logger.info(`Scheduled next fetch to within ${minutesToNextHalfHour}:${secondsToNextHalfHour}min`)
    nextFetchTimeout = setTimeout(() => {
        processFetchCycle().then(scheduleNextFetch);
    }, msToNextFetch);
}

function formatUnixTimeToCET(timestamp) {
    return moment.unix(timestamp).tz('Europe/Berlin').format('YYYY-MM-DD HH:mm:ss');
}

async function processFetchCycle() {
	const logger = getCurrentLogger();
	logger.info('Closing Fetch Intervals...')
    const results = await Promise.all(devices.map(device => device.closeInterval()));
	const insertRecords = [];
    const updateRecords = [];
    let logEntry = 'Verbrauchswerte:';
    // Verarbeite die Ergebnisse und schreibe sie in die Datenbank
	results.forEach((result, index) => {
        if (result) {
            const device = devices[index];
            const resultTS = formatUnixTimeToCET(result.timestamp);
			insertRecords.push([device.id, resultTS, result.duration, result.consumption])
            updateRecords.push(index + 1)
            logEntry += ` ${device.name}=${parseFloat(result.consumption).toFixed(2)}Wh;`;
        } else {
            logger.warning(`Gerät ${devices[index].name}: Keine Daten aufgrund eines Verbindungsfehlers.`);
        }
    });
    if (logEntry !== 'Verbrauchswerte:') {
        logger.info(logEntry);
    }
	if (insertRecords.length > 0) {
		db.query(insertQuery, [insertRecords], (error, results) => {
			if (error) {
				logger.warning('Fehler beim Einfügen der Daten:', error);
				return;
			}
			logger.info('Verbrauchswerte erfolgreich in Tabelle meter_readings eingefügt');
		});
	}
    if (updateRecords.length > 0) {
        updateRecords.forEach(deviceID => {
            db.query(updateQuery, [deviceID], (error, results) => {
                if (error) {
                    logger.warning(`(${deviceID})Fehler beim Aktualisieren der latest_fetch Zeit: ${error}`);
                    return;
                }
            });
        });
        logger.info('latest_fetch für Zählgeräte in Tabelle meters aktualisiert.');
	}
}

process.on('exit', () => db.end());

app.listen(PORT, () => console.log(`Server läuft auf Port ${PORT}`));
