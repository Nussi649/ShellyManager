const { fetch1PMData, fetchPMMiniData, fetchDimmer2Data } = require('./apiHandler');

class Device {
  	constructor({ id, name, type, address, logger }) {
		this.id = id;
		this.name = name;
		this.type = type;
		this.address = address;
		this.logger = logger;
		this.fetchFunction = this.assignFetchFunction();
		this.intervalStart = null; // UNIX-Zeitstempel in Sekunden
		this.counterStart = 0;
		this.isReachable = true;
		this.errorStart = null; // JavaScript `Date` Objekt
  	}

  	assignFetchFunction() {
      	switch (this.type) {
			case '1PM':
			case '1PMMini':
			case 'PlugS':
				return fetch1PMData;
			case 'PMMini':
				return fetchPMMiniData;
			case 'Dimmer2':
				return fetchDimmer2Data;
			default:
				this.logger.device(`[${this.name}] Unbekannter Gerätetyp: ${this.type}`)
				throw new Error(`Unbekannter Gerätetyp: ${this.type}`);
      	}
  	}

	async fetchData() {
        try {
			this.logger.device(`[${this.name}] Abruf gestartet.`)
			const { meterReading, timestamp } = await this.fetchFunction(this.address);
			if (!this.isReachable) {
				const errorDuration = (Date.now() - this.errorStart) / 1000
				this.isReachable = true;
				this.errorStart = null;
				this.logger.device(`[${this.name}] Verbindung erfolgreich wieder hergestellt. Registrierte Downtime: ${errorDuration / 60}m ${errorDuration % 60}s`)
			}
			return { meterReading, timestamp }
		} catch (error) {
			if (this.isReachable) {
				this.errorStart = Date.now();
				this.logger.device(`[${this.name}] Fehler beim Abrufen der Daten: ${error.message}`);
			}
			this.isReachable = false;
		}
		return null;
    }

	// Funktion setzt ein möglicherweise noch laufendes Intervall zurück
  	attemptStartInterval(retryCount = 0) {
		this.fetchData().then(result => {
			if (result) {
				const { meterReading, timestamp } = result;
				this.counterStart = meterReading;
				this.intervalStart = parseInt(timestamp); // Sicherstellen, dass es ein Integer ist
			} else if (retryCount < 5) {
				this.logger.device(`[${this.name}] Plane erneuten Abrufversuch (${retryCount + 1}) in 3 Minuten.`);
				setTimeout(() => this.attemptStartInterval(retryCount + 1), 3 * 60 * 1000); // Warte 5 Minuten
			}
		})
  	}

  	async closeInterval() {
      	if (this.intervalStart === null) {
			this.logger.warning(`[${this.name}] Intervall wurde noch nicht gestartet.`);
			return;
      	}
		const finalFetch = await this.fetchData();
		if (!finalFetch) { // Keine Verbindung hergestellt
			this.logger.warning(`[${this.name}] Kein Intervallabschluss möglich.`);
			return; // nichts zurück geben, intervall weiter laufen lassen
		}
		const { meterReading, timestamp } = finalFetch;
		const amount = meterReading - this.counterStart;
		const intervalDuration = timestamp - this.intervalStart;
		const result = {
			timestamp: this.intervalStart,
			duration: intervalDuration,
			consumption: amount
		};
		this.intervalStart = timestamp;
		this.counterStart = meterReading;
		return result;
	}

	async getStatus() {
		const fetchedData = await this.fetchData();
		const response = {
			id: this.id,
			name: this.name,
			type: this.type,
			running: this.intervalStart !== null,
			isReachable: this.isReachable,
            errorDuration: this.isReachable ? 0 : (Date.now() - this.errorStart) / (1000 * 60)
		}
		if (fetchedData) {
			const { meterReading, timestamp } = fetchedData;
			response['duration'] = this.intervalStart ? timestamp - this.intervalStart : 0;
            response['currentAmount'] = meterReading - this.counterStart;
		}

        return response;
    }
}

module.exports = Device;