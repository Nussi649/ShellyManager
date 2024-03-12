const winston = require('winston');
const moment = require('moment-timezone');
const path = require('path');

// Definiere benutzerdefinierte Log-Levels
const customLevels = {
    levels: {
        error: 0,
        warning: 1,
        info: 2,
        device: 3
    },
    colors: {
        error: 'red',
        warning: 'yellow',
        info: 'green',
        device: 'blue'
    }
};

// Funktion zum Erstellen eines dynamischen Loggers
function createLogger(fetchTimestamp) {
    const filename = fetchTimestamp ? `fetch_${fetchTimestamp}.log` : 'default.log';
    const filePath = path.join(__dirname, 'logs', filename);

    return winston.createLogger({
        levels: customLevels.levels,
        format: winston.format.combine(
            winston.format.timestamp({
                format: () => moment().tz('Europe/Berlin').format('YYYY-MM-DD HH:mm:ss')
            }),
            winston.format.printf(info => `${info.timestamp} ${info.level}: ${info.message}`)
        ),
        transports: [
            new winston.transports.Console({
                format: winston.format.combine(
                    winston.format.colorize(),
                    winston.format.printf(info => `${info.timestamp} ${info.level}: ${info.message}`)
                )
            }),
            new winston.transports.File({ filename: filePath })
        ]
    });
}

// Setze winston Farben basierend auf den benutzerdefinierten Levels
winston.addColors(customLevels.colors);

// Exportiere die Funktion zum Erstellen eines dynamischen Loggers
module.exports = { createLogger };
