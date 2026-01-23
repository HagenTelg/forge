$(document).ready(function() {
    const eventsTable = document.getElementById('event_log');

    function sortTable(column, reverse) {
        let rows = Array.from(eventsTable.rows);
        rows.sort((a, b) => {
            const ca = a.children[column].textContent;
            const cb = b.children[column].textContent;
            if (ca < cb) {
                return reverse ? 1 : -1;
            } else if (ca > cb) {
                return reverse ? -1 : 1;
            }
            return 0;
        });
        rows.forEach(row => eventsTable.appendChild(row));
    }
    $('.sort-down').click(function(event) {
        event.preventDefault();
        sortTable($(this).attr('sort') * 1, false);
    });
    $('.sort-up').click(function(event) {
        event.preventDefault();
        sortTable($(this).attr('sort') * 1, true);
    });

    function showLoading() { document.getElementById('loading').style.display = 'flex'; }
    function hideLoading() { document.getElementById('loading').style.display = 'none'; }

    function eventFromRow(tr) { return tr._event; }
    function setRowEvent(tr, event) { tr._event = event; }

    const showAcquisitionEvents = document.getElementById('show_acquisition_events');
    const exportCSV = document.getElementById('export_csv');
    const exportJSON = document.getElementById('export_json');

    function setHeaderVisibility() {
        if (showAcquisitionEvents.checked) {
            $('tr.event-header th:nth-child(2)').removeClass('hidden');
        } else {
            $('tr.event-header th:nth-child(2)').addClass('hidden');
        }
    }

    function setExportTimes(a) {
        a.href = $(a).attr('base') + "?start=" + TimeSelect.start_ms + '&end=' + TimeSelect.end_ms;
        if (showAcquisitionEvents.checked) {
            a.href = a.href + '&acquisition';
        }
    }

    function isEventHidden(event) {
        if (!showAcquisitionEvents.checked) {
            if (event.acquisition) {
                return true;
            }
        }
        return false;
    }
    function setRowVisibility(tr) {
        if (showAcquisitionEvents.checked) {
            tr.children[1].classList.remove('hidden');
        } else {
            tr.children[1].classList.add('hidden');
        }

        if (isEventHidden(eventFromRow(tr))) {
            tr.classList.add('hidden');
        } else {
            tr.classList.remove('hidden');
        }
    }

    function setAllRowsVisibility() {
        for (let i=0; i<eventsTable.rows.length; i++) {
            setRowVisibility(eventsTable.rows[i]);
        }
    }
    $('#show_acquisition_events').click(function(event) {
        setHeaderVisibility();
        setAllRowsVisibility();
        setExportTimes(exportCSV);
        setExportTimes(exportJSON);
    });

    setHeaderVisibility();
    setExportTimes(exportCSV);
    setExportTimes(exportJSON);


    function rowSelected(selected) {
        if (selected.classList.contains('selected')) {
            clearEventSelection();
            return
        }
        for (let i=0; i<eventsTable.rows.length; i++) {
            const tr = eventsTable.rows[i];
            tr.classList.remove('selected');
        }
        selected.classList.add('selected');
        $('.requires-selected').removeAttr('disabled');

        const event = eventFromRow(selected);
        PlotInteraction.notifyEventSelected(event);
    }
    function clearEventSelection() {
        for (let i=0; i<eventsTable.rows.length; i++) {
            const tr = eventsTable.rows[i];
            tr.classList.remove('selected');
        }
        $('.requires-selected').attr('disabled', 'disabled');
        PlotInteraction.notifyEventSelected(null);
    }

    function updateRow(tr, event) {
        setRowEvent(tr, event);

        tr.children[0].textContent = TimeParse.toDisplayTime(event.epoch_ms, ' ', ' ');
        tr.children[1].textContent = event.type || "";
        tr.children[2].textContent = event.author || "";
        tr.children[3].textContent = event.message || "";

        if (event.acquisition) {
            tr.classList.add('event-acquisition-type');
        } else {
            tr.classList.remove('event-acquisition-type');
        }
        if (event.error) {
            tr.classList.add('event-system-error');
        } else {
            tr.classList.remove('event-system-error');
        }

        setRowVisibility(tr);
    }
    function addEventRow() {
        const tr = eventsTable.insertRow();
        for (let i=0; i<4; i++) {
            tr.insertCell();
        }
        $(tr).click(function(event) {
            rowSelected(this);
        });
        return tr;
    }
    function addEventToTable(event) {
        const tr = addEventRow();
        updateRow(tr, event);
        return tr;
    }

    const EveventStream = class extends DataSocket.Stream {
        constructor() {
            super('{{ mode_name }}-events');
            showLoading();
        }

        startOfData() {
            while (eventsTable.rows.length > 0) {
                eventsTable.deleteRow();
            }
            $('.requires-selected').attr('disabled', 'disabled');
        }

        endOfData() {
            hideLoading();
        }

        incomingDataContent(content) {
            addEventToTable(content);
        }
    };
    (new EveventStream()).beginStream();
});
