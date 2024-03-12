document.getElementById('toggleFetch').addEventListener('click', toggleFetch);
document.getElementById('refreshDevices').addEventListener('click', refreshDevices);
document.addEventListener('DOMContentLoaded', () => fetchDeviceStatus());
const statusButton = document.getElementById('getStatus');

statusButton.addEventListener('click', fetchDeviceStatus);

function toggleFetch() {
    const action = this.textContent === 'Start' ? 'start-fetch' : 'stop-fetch';
    fetch(`/${action}`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
    }).then(response => response.json())
        .then(data => {
            if (data.status === 'started') {
                this.textContent = 'Stop';
                toggleStatusButton(true);
                fetchDeviceStatus();
            } else if (data.status === 'stopped') {
                this.textContent = 'Start';
                toggleStatusButton(false);
                document.getElementById('devices').innerHTML = '';  // clear devicesDiv
            }
        });
}

function toggleStatusButton(enabled) {
    statusButton.disabled = !enabled;
}

function fetchDeviceStatus() {
    fetch('/status')
        .then(response => response.json())
        .then( data => {
            const toggleBtn = document.getElementById('toggleFetch');
            if (data.fetching) {
                if (toggleBtn.textContent === 'Start') {
                    toggleBtn.textContent = 'Stop';
                    toggleStatusButton(true);
                }
            } else {
                if (toggleBtn.textContent === 'Stop') {
                    toggleBtn.textContent = 'Start';
                    toggleStatusButton(false)
                }
            }
            updateDeviceStatus(data.devices);})
}

function refreshDevices() {
    fetch('/refresh')
        .then(response => {
            if (response.ok) {
                return response.json();
            } else {
                throw new Error('Ein Fehler ist aufgetreten beim Aktualisieren der Geräte.');
            }
        })
        .then(() => {
            fetchDeviceStatus();
        })
        .catch(error => console.error(error));
}

function updateDeviceStatus(devices) {
    const devicesDiv = document.getElementById('devices');
    devicesDiv.innerHTML = ''; // Clear current content
    devices.forEach(device => {
        const deviceDiv = document.createElement('div');
        deviceDiv.classList.add('device');
        deviceDiv.innerHTML = `
            <strong>${device.name}</strong> (${device.type}) - 
            <span class="device-status">${device.running ? 'Aktiv' : 'Inaktiv'}</span>
            ${device.isReachable ? '' : '<span class="device-error"> (Verbindungsfehler)</span>'}
        `;
        if (device.running) {
            const runningInfo = document.createElement('div');
            const formattedAmount = parseFloat(device.currentAmount).toFixed(2);
            const runningDuration = `${formattedAmount} Wh seit ${padZero(Math.floor(device.duration / 60))}:${padZero(device.duration % 60)} min.`;
            runningInfo.innerHTML = runningDuration;
            deviceDiv.appendChild(runningInfo);
        }
        if (!device.isReachable) {
            const errorInfo = document.createElement('div');
            errorInfo.innerHTML = `Seit ${padZero(device.errorDuration)} min nicht erreichbar.`;
            deviceDiv.appendChild(errorInfo);
        }
        devicesDiv.appendChild(deviceDiv);
    });
}

function padZero(num) {
    return num.toString().padStart(2, '0');
}