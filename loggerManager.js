const { createLogger } = require('./logger');
const moment = require('moment-timezone');

let currentLogger = null;

function startNewFetchLogger() {
    const fetchTimestamp = moment().tz('Europe/Berlin').format('YYYY-MM-DD HH:mm:ss');
    currentLogger = createLogger(fetchTimestamp);
    return currentLogger;
}

function getCurrentLogger() {
    return currentLogger;
}

function clearCurrentLogger() {
    currentLogger = createLogger();
}

module.exports = { startNewFetchLogger, getCurrentLogger, clearCurrentLogger };
