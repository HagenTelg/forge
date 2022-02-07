const ROOT_URL = "{{ request.url_for('root') }}";

localStorage.setItem('forge-last-station', '{{ station }}');
localStorage.setItem('forge-last-mode', '{{ mode.mode_name }}');

$('a.view-select').click(function(event) {
    event.preventDefault();

    DataSocket.resetLoadedRecords();
    TimeSelect.resetZoomConnections();
    TimeSelect.resetIntervalHeartbeat();

    $('a.view-select').not(this).removeClass('active');
    $(this).addClass('active');
    localStorage.setItem('forge-last-view', $(this).attr('name'));
    $('#view_content').load($(this).attr('interior'));
});

$(document).ready(function(event) {
    const queryParameters = new URLSearchParams(window.location.search);
    let selectView = queryParameters.get('view');
    if (selectView === null) {
        selectView = localStorage.getItem('forge-last-view');
    }

    if (selectView !== null) {
        $("a.view-select[name='" + selectView +"']").click();
    }
    if ($('a.view-select.active').length === 0) {
        $('a.view-select').first().click();
    }
});


let eventLog = null;
$('#show_event_log').click(function(event) {
    event.preventDefault();

    if (!eventLog || eventLog.closed) {
        const eventsURL = '{{ request.url_for("eventlog", station=station, mode_name=mode.mode_name) }}';
        eventLog = window.open(eventsURL,
            'EventLog',
            'width=900,height=500,menubar=0,toolbar=0,location=0,status=0,resizable=1,scrollbars=1');

        eventLog.onunload = function() { TimeSelect.highlight('EventLog'); }
    } else {
        eventLog.focus();
    }
});

window.addEventListener("message", (event) => {
    if (event.source !== eventLog || !ROOT_URL.startsWith(event.origin) ||
            !event.source.location.href.startsWith(ROOT_URL)) {
        return;
    }
    const data = event.data;
    if (data.type === "EventLogSelected") {
        const selectedEvent = data.event;
        if (!selectedEvent) {
            TimeSelect.highlight('EventLog');
            return;
        }

        TimeSelect.highlight('EventLog',
            selectedEvent.epoch_ms,
            selectedEvent.epoch_ms,
            2);
    }
});


let savedZoom = [];
function restoreSavedZoom() {
    while (savedZoom.length > 0) {
        const start_ms = savedZoom[savedZoom.length-1].start_ms;
        const end_ms = savedZoom[savedZoom.length-1].end_ms;
        savedZoom.pop();

        if (start_ms === TimeSelect.zoom_start_ms && end_ms === TimeSelect.zoom_end_ms) {
            continue;
        }

        TimeSelect.applyZoom(start_ms, end_ms);
        updateSavedZoomDisplay();
        return;
    }

    TimeSelect.resetTimeRange();
    updateSavedZoomDisplay();
}
function updateSavedZoomDisplay() {
    if (savedZoom.length === 0) {
        document.getElementById('time_history_container').style.display = 'none';
        return;
    }

    document.getElementById('time_history_container').style.display = 'block';
    document.getElementById("time_history_saved").textContent =
        TimeParse.toDisplayTime(savedZoom[savedZoom.length-1].start_ms) + " to " +
        TimeParse.toDisplayTime(savedZoom[savedZoom.length-1].end_ms);
}
function saveCurrentZoom() {
    if (!TimeSelect.isZoomed()) {
        return;
    }
    if (savedZoom.length !== 0) {
        if (TimeSelect.zoom_start_ms === savedZoom[savedZoom.length-1].start_ms &&
                TimeSelect.zoom_end_ms === savedZoom[savedZoom.length-1].end_ms) {
            return;
        }
    }
    savedZoom.push({
        start_ms: TimeSelect.zoom_start_ms,
        end_ms: TimeSelect.zoom_end_ms,
    });
    updateSavedZoomDisplay();
}
$('#time_history_container').click(function(event) {
    event.preventDefault();
    restoreSavedZoom();
});


$(document).ready(function(event) {
    TimeSelect.fetchLatestPassed = function() {
        return $.get("{{ request.url_for('latest_passed', station=station, mode_name=mode.mode_name) }}");
    }

    if (localStorage.getItem('forge-settings-plot-autosave-zoom')) {
        TimeSelect.onZoom('ViewListAutosaveZoom', () => {
            saveCurrentZoom();
        });
    }
});

function isModalActive() {
    const modal = document.getElementById('modal-container');
    return modal.style.display === 'block';
}

function cycleSelected(offset) {
    const active = $('a.view-select.active').first().get(0);
    const selectors = $('a.view-select').get();
    for (let i=0; i<selectors.length; i++) {
        if (selectors[i] !== active) {
            continue;
        }

        i = i + offset;
        if (i < 0) {
            i = selectors.length - 1;
        } else if (i >= selectors.length) {
            i = 0;
        }

        $(selectors[i]).click();
        return;
    }

    if (offset < 0) {
        $('a.view-select').last().click();
    } else {
        $('a.view-select').first().click();
    }
}

$(document).keydown(function(event) {
    if (isModalActive()) {
        return;
    }
    if (event.target.matches('input') || event.target.matches('textarea')) {
        return;
    }

    switch(event.code) {
    case 'KeyN':
        event.preventDefault();

        cycleSelected(1);
        break;

    case 'KeyP':
    case 'KeyB':
        event.preventDefault();

        cycleSelected(-1);
        break;

    case 'KeyR':
        event.preventDefault();

        //{% if not realtime %}
        if (TimeSelect.isZoomed()) {
            TimeSelect.change(TimeSelect.zoom_start_ms, TimeSelect.zoom_end_ms);
        } else {
            DataSocket.reloadData();
        }
        //{% else %}
        DataSocket.reloadData();
        //{% endif %}
        break;

    case 'KeyA':
        event.preventDefault();

        //{% if not realtime %}
        TimeSelect.resetTimeRange();
        //{% else %}
        TimeSelect.resetInterval();
        //{% endif %}
        break;

    case 'KeyZ':
        event.preventDefault();

        restoreSavedZoom();
        break;
    case 'KeyX':
        event.preventDefault();

        saveCurrentZoom();
        break;
    }
});