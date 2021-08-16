const startTimeEntry = document.getElementById("input-start-time");
const startTimeDisplay = document.getElementById("parsed-start-time");
const endTimeEntry = document.getElementById("input-end-time");
const endTimeDisplay = document.getElementById("parsed-end-time");
const exportButton = document.getElementById("export-data");

$(exportButton).click(function(event) {
    event.preventDefault();
    exportButton.disabled = true;

    let parsedStart = TimeParse.parseTime(startTimeEntry.value, TimeSelect.end_ms, -1);
    const parsedEnd = TimeParse.parseTime(endTimeEntry.value, parsedStart, 1);
    parsedStart = TimeParse.parseTime(startTimeEntry.value, parsedEnd, -1);

    if (!parsedStart || !parsedEnd) {
        hideModal();
        return;
    }

    const key = document.getElementById("export-type").value;

    window.open("{{ request.url_for('export_data', station=station, mode_name=mode_name) }}" +
        '?start=' + parsedStart +
        '&end=' + parsedEnd +
        '&key=' + key);
    hideModal();
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
    if (parsedEnd) {
        exportButton.disabled = false;
    }

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
    if (parsedStart) {
        exportButton.disabled = false;
    }

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

exportButton.disabled = false;