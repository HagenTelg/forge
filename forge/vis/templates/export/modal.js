const exportSocket = new WebSocket("{{ request.url_for('export_socket', station=station) }}");

const startTimeEntry = document.getElementById("input-start-time");
const startTimeDisplay = document.getElementById("parsed-start-time");
const endTimeEntry = document.getElementById("input-end-time");
const endTimeDisplay = document.getElementById("parsed-end-time");
const exportButton = document.getElementById("export-data");

function exportDownloadURL() {
    let parsedStart = TimeParse.parseTime(startTimeEntry.value, TimeSelect.end_ms, -1);
    const parsedEnd = TimeParse.parseTime(endTimeEntry.value, parsedStart, 1);
    parsedStart = TimeParse.parseTime(startTimeEntry.value, parsedEnd, -1);

    if (!parsedStart || !parsedEnd) {
        return undefined;
    }

    const key = document.getElementById("export-type").value;

    return "{{ request.url_for('export_data', station=station, mode_name=mode_name) }}" +
        '?start=' + parsedStart +
        '&end=' + parsedEnd +
        '&key=' + key;
}

function startExportDownload(filename, size) {
    $('#export-waiting').addClass('hidden');
    $("#export-ready").removeClass('hidden');
    $('#export-cancel').text("Close");

    const url = exportDownloadURL();
    if (!url) {
        hideModal();
        return;
    }

    const units = ["B", "KiB", "MiB", "GiB", "TiB"];
    let divisor = 1;
    let formattedSize;
    for (let i=0; i<units.length; i++, divisor *= 1024) {
        let dividedSize = size / divisor;
        if (divisor === 1) {
            formattedSize = dividedSize.toFixed(0);
        } else if (dividedSize <= 9.99) {
            formattedSize = dividedSize.toFixed(2);
        } else if (dividedSize <= 99.9) {
            formattedSize = dividedSize.toFixed(1);
        } else {
            formattedSize = dividedSize.toFixed(0);
        }
        formattedSize = formattedSize + " " + units[i];
        if (dividedSize <= 999.0) {
            break;
        }
    }

    const link = document.getElementById('export-download-link');
    link.href = url;
    link.download = filename;
    link.textContent = filename + " (" + formattedSize + ")";
    link.click();
}

function showExportWaiting() {
    exportButton.classList.add('hidden');
    $('#export-parameters').addClass('hidden');
    $("#export-waiting").removeClass('hidden');
}

$(exportButton).click(function(event) {
    event.preventDefault();

    let parsedStart = TimeParse.parseTime(startTimeEntry.value, TimeSelect.end_ms, -1);
    const parsedEnd = TimeParse.parseTime(endTimeEntry.value, parsedStart, 1);
    parsedStart = TimeParse.parseTime(startTimeEntry.value, parsedEnd, -1);

    if (!parsedStart || !parsedEnd) {
        hideModal();
        return;
    }

    showExportWaiting();

    const key = document.getElementById("export-type").value;
    exportSocket.addEventListener('message', (event) => {
        const reply = JSON.parse(event.data);
        if (reply.type === 'ready') {
            const filename = reply.filename;
            const size = reply.size;
            startExportDownload(filename, size);
        }
    });
    exportSocket.send(JSON.stringify({
        action: 'wait',
        mode: '{{ mode_name }}',
        key: key,
        start_epoch_ms: parsedStart,
        end_epoch_ms: parsedEnd,
    }));
});
$('#export-cancel').click(function(event) {
    event.preventDefault();
    hideModal();
});

let haveEditedStart = false;
let haveEditedEnd = false;

function startTimeEdited() {
    haveEditedStart = true;

    const parsedEnd = TimeParse.parseTime(endTimeEntry.value, TimeSelect.start_ms, 1);
    const parsedStart = TimeParse.parseTime(startTimeEntry.value, parsedEnd, -1);

    if (!parsedStart) {
        startTimeEntry.classList.add('invalid');
        startTimeDisplay.classList.add('invalid');

        startTimeDisplay.textContent = "ERROR";
        exportButton.disabled = true;
        return;
    }
    exportButton.disabled = !(parsedEnd && parsedStart < parsedEnd && (exportSocket.readyState === 1));

    startTimeEntry.classList.remove('invalid');
    startTimeDisplay.classList.remove('invalid');
    startTimeDisplay.textContent = TimeParse.toDisplayTime(parsedStart);

    if (!haveEditedEnd) {
        const offset = TimeParse.getImpliedOffset(startTimeEntry.value, parsedStart);
        const setTime = TimeParse.parseTime(offset, parsedStart, 1);
        if (setTime) {
            endTimeEntry.classList.remove('invalid');
            endTimeDisplay.classList.remove('invalid');
            endTimeEntry.value = offset;
            endTimeDisplay.textContent = TimeParse.toDisplayTime(setTime);
        }
    }
}
$('#input-start-time').change(startTimeEdited);
$('#input-start-time').on('input', startTimeEdited);
startTimeEntry.value = startTimeDisplay.textContent = TimeParse.toDisplayTime(TimeSelect.start_ms);

function endTimeEdited() {
    haveEditedEnd = true;

    const parsedStart = TimeParse.parseTime(startTimeEntry.value, TimeSelect.end_ms, -1);
    const parsedEnd = TimeParse.parseTime(endTimeEntry.value, parsedStart, 1);

    if (!parsedEnd) {
        endTimeEntry.classList.add('invalid');
        endTimeDisplay.classList.add('invalid');

        endTimeDisplay.textContent = "ERROR";
        exportButton.disabled = true;
        return;
    }
    exportButton.disabled = !(parsedStart && parsedStart < parsedEnd && (exportSocket.readyState === 1));

    endTimeEntry.classList.remove('invalid');
    endTimeDisplay.classList.remove('invalid');
    endTimeDisplay.textContent = TimeParse.toDisplayTime(parsedEnd);

    if (!haveEditedStart) {
        const offset = TimeParse.getImpliedOffset(endTimeEntry.value, parsedEnd);
        const setTime = TimeParse.parseTime(offset, parsedEnd, -1);
        if (setTime) {
            startTimeEntry.classList.remove('invalid');
            startTimeDisplay.classList.remove('invalid');
            startTimeEntry.value = offset;
            startTimeDisplay.textContent = TimeParse.toDisplayTime(setTime);
        }
    }
}
$('#input-end-time').change(endTimeEdited);
$('#input-end-time').on('input', endTimeEdited);
endTimeEntry.value = endTimeDisplay.textContent = TimeParse.toDisplayTime(TimeSelect.end_ms);

exportButton.disabled = true;
exportSocket.addEventListener('open', (event) => {
    let parsedStart = TimeParse.parseTime(startTimeEntry.value, TimeSelect.end_ms, -1);
    const parsedEnd = TimeParse.parseTime(endTimeEntry.value, parsedStart, 1);
    parsedStart = TimeParse.parseTime(startTimeEntry.value, parsedEnd, -1);
    exportButton.disabled = !(parsedEnd && parsedStart);
});
const modalWindow = document.getElementById('modal-container');
const originalHide = modalWindow.onmodalhide;
modalWindow.onmodalhide = function() {
    exportSocket.close();
    if (originalHide) {
        originalHide();
    }
};