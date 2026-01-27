function hideLoading() { document.getElementById('loading').style.display = 'none'; }

const queryParameters = new URLSearchParams(window.location.search);

let selectedStations = localStorage.getItem('forge-dashboard-stations');
if (selectedStations) {
    selectedStations = new Set(selectedStations.split(' '));
} else {
    selectedStations = new Set();
}
const querySelectedStation = queryParameters.get('station');
if (querySelectedStation !== null) {
    selectedStations.clear();
    selectedStations.add(querySelectedStation);
}

const availableStations = new Set();
function saveSelectedStations() {
    if (selectedStations.size === availableStations.size) {
        localStorage.removeItem('forge-dashboard-stations');
    } else {
        localStorage.setItem('forge-dashboard-stations', Array.from(selectedStations).join(' '));
    }
}

function updateSelectedStations() {
    selectedStations.clear();
    $('#station_select > ul.menu-list input').each(function() {
        if (this.checked) {
            selectedStations.add(this.station);
        }
    });
}

function updateStationSelectedDisplay() {
    const stationFilter = document.getElementById('station_select');
    const stationFilterTitle = $('> button.dropdown-button', stationFilter)

    if (availableStations.size === 0) {
        stationFilterTitle.text("Station");
        stationFilter.classList.add('disabled');
        return;
    }
    if (availableStations.size === 1) {
        const station = availableStations.values().next().value;
        if (station === '_') {
            stationFilterTitle.text("No Station");
        } else {
            stationFilterTitle.text(station.toUpperCase());
        }
        stationFilter.classList.add('disabled');
        return;
    }

    stationFilter.classList.remove('disabled');

    if (selectedStations.size === 1) {
        const station = selectedStations.values().next().value;
        if (station === '_') {
            stationFilterTitle.text("Station: None");
        } else {
            stationFilterTitle.text("Station: " + station.toUpperCase());
        }
        return;
    }

    stationFilterTitle.text("Station");
}

$('#station_select .menu-select-all').click(function(e) {
    e.preventDefault();

    let allChecked = true;
    $('#station_select > ul.menu-list input').each(function() {
        if (!this.checked) {
            allChecked = false;
        }
    });
    if (allChecked) {
        selectedStations.clear();
        $('#station_select > ul.menu-list input').prop('checked', false);
    } else {
        for (const station of availableStations) {
            selectedStations.add(station);
        }
        $('#station_select > ul.menu-list input').prop('checked', true);
    }

    updateStationSelectedDisplay();
    saveSelectedStations();
    updateVisibleEntries();
})

function populateStations(availableEntries) {
    const stationFilterList = document.querySelector('#station_select > ul.menu-list');

    availableStations.clear();
    for (const entry of availableEntries) {
        availableStations.add(entry.station.toLowerCase());
    }
    if (availableStations.size <= 1) {
        selectedStations.clear();
    } else {
        selectedStations.forEach((station) => {
            if (!availableStations.has(station)) {
                selectedStations.delete(station);
            }
        });
    }
    if (selectedStations.size === 0) {
        for (const station of availableStations) {
            selectedStations.add(station);
        }
    }

    while (stationFilterList.children.length > 1) {
        stationFilterList.removeChild(stationFilterList.children[1]);
    }

    function addStationSelector(station, text) {
        const li = document.createElement('li');
        const label = document.createElement('label');
        li.appendChild(label);
        const checkbox = document.createElement('input');
        checkbox.type = 'checkbox';
        checkbox.checked = selectedStations.has(station);
        checkbox.station = station;
        label.appendChild(checkbox);
        label.appendChild(document.createTextNode(text));

        $(checkbox).click(function() {
            updateSelectedStations();
            updateStationSelectedDisplay();
            saveSelectedStations();
            updateVisibleEntries();
        });

        stationFilterList.appendChild(li);
    }

    if (availableStations.has('')) {
        addStationSelector('', "None")
    }
    const sortedStations = Array.from(availableStations);
    sortedStations.sort();
    for (const station of sortedStations) {
        if (station === '') {
            continue;
        }
        addStationSelector(station, station.toUpperCase());
    }

    updateStationSelectedDisplay();
}


let selectedCodes = localStorage.getItem('forge-dashboard-codes');
if (selectedCodes) {
    selectedCodes = new Set(selectedCodes.split(' '));
} else {
    selectedCodes = new Set();
}
const availableCodes = new Set();
const codeDisplayText = new Map();
function saveSelectedCodes() {
    if (selectedCodes.size === availableCodes.size) {
        localStorage.removeItem('forge-dashboard-codes');
    } else {
        localStorage.setItem('forge-dashboard-codes', Array.from(selectedCodes).join(' '));
    }
}

function updateSelectedCodes() {
    selectedCodes.clear();
    $('#code_select > ul.menu-list input').each(function() {
        if (this.checked) {
            selectedCodes.add(this.code);
        }
    });
}

function updateCodeSelectedDisplay() {
    const codeFilter = document.getElementById('code_select');
    const codeFilterTitle = $('> button.dropdown-button', codeFilter)

    if (availableCodes.size === 0) {
        codeFilterTitle.text("Type");
        codeFilter.classList.add('disabled');
        return;
    }
    if (availableCodes.size === 1) {
        const code = availableCodes.values().next().value;
        let display = codeDisplayText.get(code);
        if (!display) {
            display = code;
        }
        codeFilterTitle.text(display);
        codeFilter.classList.add('disabled');
        return;
    }

    codeFilter.classList.remove('disabled');

    if (selectedCodes.size === 1) {
        const code = selectedCodes.values().next().value;
        let display = codeDisplayText.get(code);
        if (!display) {
            display = code;
        }
        codeFilterTitle.text("Type: " + display);
        return;
    }

    codeFilterTitle.text("Type");
}

$('#code_select .menu-select-all').click(function(e) {
    e.preventDefault();

    let allChecked = true;
    $('#code_select > ul.menu-list input').each(function() {
        if (!this.checked) {
            allChecked = false;
        }
    });
    if (allChecked) {
        selectedCodes.clear();
        $('#code_select > ul.menu-list input').prop('checked', false);
    } else {
        for (const code of availableCodes) {
            selectedCodes.add(code);
        }
        $('#code_select > ul.menu-list input').prop('checked', true);
    }

    updateCodeSelectedDisplay();
    saveSelectedCodes();
    updateVisibleEntries();
})

function populateCodes(availableEntries) {
    const codeFilterList = document.querySelector('#code_select > ul.menu-list');

    availableCodes.clear();
    codeDisplayText.clear();
    for (const entry of availableEntries) {
        const code = entry.code.toLowerCase();
        availableCodes.add(code);
        const display = entry.display;
        if (display) {
            codeDisplayText.set(code, display);
        }
    }

    if (availableCodes.size <= 1) {
        selectedCodes.clear();
    } else {
        selectedCodes.forEach((code) => {
            if (!availableCodes.has(code)) {
                selectedCodes.delete(code);
            }
        });
    }
    if (selectedCodes.size === 0) {
        for (const code of availableCodes) {
            selectedCodes.add(code);
        }
    }

    while (codeFilterList.children.length > 1) {
        codeFilterList.removeChild(codeFilterList.children[1]);
    }

    function addCodeSelector(code, text) {
        const li = document.createElement('li');
        const label = document.createElement('label');
        li.appendChild(label);
        const checkbox = document.createElement('input');
        checkbox.type = 'checkbox';
        checkbox.checked = selectedCodes.has(code);
        checkbox.code = code;
        label.appendChild(checkbox);
        label.appendChild(document.createTextNode(text));

        $(checkbox).click(function() {
            updateSelectedCodes();
            updateCodeSelectedDisplay();
            saveSelectedCodes();
            updateVisibleEntries();
        });

        codeFilterList.appendChild(li);
    }

    const sortedCodes = Array.from(availableCodes);
    sortedCodes.sort();
    for (const code of sortedCodes) {
        let display = codeDisplayText.get(code);
        if (!display) {
            display = code;
        }
        addCodeSelector(code, display);
    }

    updateCodeSelectedDisplay();
}


const currentVisitTime = Date.now();
let lastUnviewedTime = localStorage.getItem('forge-dashboard-unviewed');
let showUnviewed = true;
if (lastUnviewedTime) {
    const parts = lastUnviewedTime.split(' ');
    lastUnviewedTime = parts[0] * 1;
    showUnviewed = !!(parts[1] * 1);
} else {
    lastUnviewedTime = currentVisitTime;
}
function saveSelectedTime() {
    localStorage.setItem('forge-dashboard-unviewed',
        currentVisitTime.toString() + ' ' +
        (showUnviewed ? '1' : '0'));
}
saveSelectedTime();

const timeSelectButton = document.getElementById("time_select");
function updateDisplayedInterval() {
    timeSelectButton.textContent = "Display: " + TimeParse.toDisplayInterval(TimeSelect.interval_ms);
    updateStartTime();
    updateVisibleEntries();
}
$(timeSelectButton).click(function(e) {
    showModal("{{ request.url_for('static', path='/modal/intervalselect.html') }}");
    e.preventDefault();
});

const unviewedButton = document.getElementById("time_new");
function updateDisplayedUnviewed() {
    if (showUnviewed) {
        unviewedButton.classList.remove('mdi-calendar-blank');
        unviewedButton.classList.add('mdi-calendar-refresh');
    } else {
        unviewedButton.classList.add('mdi-calendar-blank');
        unviewedButton.classList.remove('mdi-calendar-refresh');
    }
}
updateDisplayedUnviewed();
$(unviewedButton).click(function(e) {
    showUnviewed = !showUnviewed;
    updateDisplayedUnviewed();
    saveSelectedTime();
    updateStartTime();
    e.preventDefault();
});
function getStartTime() {
    const now = Date.now();
    let start_ms = now - TimeSelect.interval_ms;
    start_ms = Math.floor(start_ms / (60 * 1000)) * 60 * 1000;
    if (showUnviewed) {
        if (lastUnviewedTime < start_ms) {
            start_ms = lastUnviewedTime;
        }
    }
    return start_ms;
}


const entryLookup = new Map();
function populateEntries(availableEntries) {
    const unavailableEntries = new Map(entryLookup);
    for (const entry of availableEntries) {
        const key = entry.station + ' ' + entry.code;
        unavailableEntries.delete(key);
        const existingEntry = entryLookup.get(key);
        if (!existingEntry) {
            entryLookup.set(key, new DashboardEntry(entry))
        } else {
            existingEntry.updateData(entry);
        }
    }
    unavailableEntries.forEach((entry, key) => {
        entryLookup.delete(key);
        entry.setUnavailable();
    });
}

function loadAvailable() {
    DashboardSocket.listEntries(function(data) {
        entryLookup.forEach((entry) => {
            entry.setReload();
        });

        populateStations(data);
        saveSelectedStations();

        populateCodes(data);
        saveSelectedCodes();

        populateEntries(data);
        updateStartTime();

        updateVisibleEntries();
        hideLoading();
    });
}
setInterval(loadAvailable, 5 * 60 * 1000);
loadAvailable();


const emailAllButton = document.getElementById("email_all");

function updateVisibleEntries() {
    Sorting.visibleEntries.length = 0;
    entryLookup.forEach((entry) => {
        if (!selectedStations.has(entry.station)) {
            entry.updateVisibility(false);
            return;
        }
        if (!selectedCodes.has(entry.code)) {
            entry.updateVisibility(false);
            return;
        }
        Sorting.visibleEntries.push(entry);
    });
    Sorting.updateTable();
    for (const entry of Sorting.visibleEntries) {
        entry.updateVisibility(true);
    }
    emailAllButton.disabled = (Sorting.visibleEntries.length === 0);
}

function updateStartTime() {
    const startTime = getStartTime();
    entryLookup.forEach((entry) => {
        entry.updateStartTime(startTime);
    });
}

TimeSelect.onChanged(timeSelectButton, updateDisplayedInterval);
updateDisplayedInterval();


$('#email_all').click(function(event) {
    event.preventDefault();

    if (Sorting.visibleEntries.length === 0) {
        return;
    }
    EMAIL_PROMPT_ENTRIES = Sorting.visibleEntries.slice();
    showModal("{{ request.url_for('static', path='/modal/dashboardemail.html') }}");
});