const axios = require('axios');

// Geräte vom Typ 1PM, 1PMMini, PlugS
async function fetch1PMData(ipAddress) {
    const response = await axios.get(`http://${ipAddress}/rpc/Switch.GetStatus?id=0`).catch(error => {
        throw new Error(`Fehler beim Abrufen der Daten: ${error.message}`);
    });
    // Extrahiere die Verbrauchswerte und den Timestamp
    const meterReading = response.data.aenergy.total;
    const timestamp = response.data.aenergy.minute_ts;
    return { meterReading, timestamp };
}

// Geräte vom Typ PMMini
async function fetchPMMiniData(ipAddress) {
    const response = await axios.get(`http://${ipAddress}/rpc/PM1.GetStatus?id=0`).catch(error => {
        throw new Error(`Fehler beim Abrufen der Daten: ${error.message}`);
    });
    // Extrahiere die Verbrauchswerte und den Timestamp
    const meterReading = response.data.aenergy.total;
    const timestamp = response.data.aenergy.minute_ts;
    return { meterReading, timestamp };
}

// Geräte vom Typ Dimmer2
async function fetchDimmer2Data(ipAddress) {
    const response = await axios.get(`http://${ipAddress}/status`).catch(error => {
        throw new Error(`Fehler beim Abrufen der Daten: ${error.message}`);
    });
    // Extrahiere die Verbrauchswerte und den Timestamp. Angenommen, es gibt nur einen Meter, daher [0] als Zugriff
    const meterReading = response.data.meters[0].total / 60.0;
    const timestamp = response.data.meters[0].timestamp - 3600;
    return { meterReading, timestamp };
}

module.exports = {
    fetch1PMData,
    fetchPMMiniData,
    fetchDimmer2Data
};