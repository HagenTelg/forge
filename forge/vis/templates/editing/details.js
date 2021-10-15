let directive = {};
let saveChanges = (directive) => {};
let saveAction = undefined;

const startTimeEntry = document.getElementById("details-input-start-time");
const startTimeDisplay = document.getElementById("details-parsed-start-time");
const endTimeEntry = document.getElementById("details-input-end-time");
const endTimeDisplay = document.getElementById("details-parsed-end-time");
const acceptButton = document.getElementById('details_accept');

const invalidLocks = new Set();
function addInvalidLock(key) {
    invalidLocks.add(key);
    acceptButton.classList.add('invalid');
}
function removeInvalidLock(key) {
    invalidLocks.delete(key);
    if (!invalidLocks.size) {
        acceptButton.classList.remove('invalid');
    }
}

function startTimeEdited() {
    const parsedEnd = TimeParse.parseTime(endTimeEntry.value, PlotInteraction.start_ms, 1);
    const parsedStart = TimeParse.parseTime(startTimeEntry.value, parsedEnd, -1);

    if (parsedStart !== null && !parsedStart) {
        startTimeEntry.classList.add('invalid');
        startTimeDisplay.classList.add('invalid');

        startTimeDisplay.textContent = "ERROR";
        addInvalidLock('start_time');
        return;
    }

    removeInvalidLock('start_time');
    startTimeEntry.classList.remove('invalid');
    startTimeDisplay.classList.remove('invalid');
    startTimeDisplay.textContent = TimeParse.toDisplayTime(parsedStart);
    directive.start_epoch_ms = parsedStart;
    PlotInteraction.notifyDirectiveSelected(directive);
}
$('#details-input-start-time').change(startTimeEdited);
$('#details-input-start-time').on('input', startTimeEdited);

function endTimeEdited() {
    const parsedStart = TimeParse.parseTime(startTimeEntry.value, PlotInteraction.end_ms, -1);
    const parsedEnd = TimeParse.parseTime(endTimeEntry.value, parsedStart, 1);

    if (parsedEnd !== null && !parsedEnd) {
        endTimeEntry.classList.add('invalid');
        endTimeDisplay.classList.add('invalid');

        endTimeDisplay.textContent = "ERROR";
        addInvalidLock('end_time');
        return;
    }

    removeInvalidLock('end_time');
    endTimeEntry.classList.remove('invalid');
    endTimeDisplay.classList.remove('invalid');
    endTimeDisplay.textContent = TimeParse.toDisplayTime(parsedEnd);
    directive.end_epoch_ms = parsedEnd;
    PlotInteraction.notifyDirectiveSelected(directive);
}
$('#details-input-end-time').change(endTimeEdited);
$('#details-input-end-time').on('input', endTimeEdited);


function setTimeBounds(start_ms, end_ms) {
    startTimeEntry.value = startTimeDisplay.textContent = TimeParse.toDisplayTime(start_ms);
    removeInvalidLock('start_time');
    startTimeEntry.classList.remove('invalid');
    startTimeDisplay.classList.remove('invalid');
    endTimeEntry.value = endTimeDisplay.textContent = TimeParse.toDisplayTime(end_ms);
    removeInvalidLock('end_time');
    endTimeEntry.classList.remove('invalid');
    endTimeDisplay.classList.remove('invalid');
}
PlotInteraction.timeSelected('edit-details', function(start_ms, end_ms) {
    setTimeBounds(start_ms, end_ms);
    directive.start_epoch_ms = start_ms;
    directive.end_epoch_ms = end_ms;
    PlotInteraction.notifyDirectiveSelected(directive);
});

function populateHistory() {
    if (!directive.history || directive.history.length === 0) {
        $('.requires-history').css('display', 'none');
        return;
    }
    $('.requires-history').css('display', '');

    const eventsTable = document.getElementById('details_history_events');
    while (eventsTable.rows.length > 0) {
        eventsTable.deleteRow();
    }

    function addRow() {
        const tr = eventsTable.insertRow();
        for (let i=0; i<3; i++) {
            tr.insertCell();
        }
        return tr;
    }

    directive.history.forEach((event) => {
        const tr = addRow();
        tr.historyEvent = event;
        tr.children[0].textContent = TimeParse.toDisplayTime(event.time_epoch_ms, ' ', ' ');
        tr.children[1].textContent = event.user;
        tr.children[2].textContent = event.operation;
    });
}

function populateEditor() {
    setTimeBounds(directive.start_epoch_ms, directive.end_epoch_ms);
    document.getElementById('details_comment').value = directive.comment;
    document.getElementById('details_author').value = directive.author;
    $('#details_action').val(directive.action).change();
    populateHistory();
}

displayEditDirectiveDetails = function(originalDirective, onsave) {
    if (originalDirective) {
        directive = JSON.parse(JSON.stringify(originalDirective));
    } else {
        directive = {
            start_epoch_ms: PlotInteraction.start_ms,
            end_epoch_ms: PlotInteraction.end_ms,
            author: "{{ request.user.initials }}",
            action: 'invalidate',
            comment: "",
        };
    }
    saveChanges = onsave;
    populateEditor();
    invalidLocks.clear();
    acceptButton.classList.remove('invalid');
    PlotInteraction.notifyDirectiveSelected(directive);
};

$('nav.details-category-select button').click(function(event) {
    const display = $($(this).attr('category'));
    $('.details-category').not(display).removeClass('active');
    display.addClass('active');

    $('nav.details-category-select button').not(this).removeClass('active');
    $(this).addClass('active');
}).first().click();

$('#details_action').change(function(event) {
    $('.details-action-description').removeClass('active');
    $('.details-action-description[code=' + this.value + ']').addClass('active');
    directive.action =  this.value;

    const selectedOption = this.options[this.selectedIndex];
    const editor = $(selectedOption).attr('editor');
    $.ajax(editor).done(function(responseText) {
        $('#details_action_content').html(responseText).ready(() => {
            saveAction = selectEditDirectiveAction(directive);
        });
    });
});

$(acceptButton).click(function(event) {
    if (invalidLocks.size) {
        event.preventDefault();
        return;
    }
    hideModal();

    const parsedStart = TimeParse.parseTime(startTimeEntry.value, PlotInteraction.end_ms, -1);
    directive.end = TimeParse.parseTime(endTimeEntry.value, parsedStart, 1);
    directive.start = TimeParse.parseTime(startTimeEntry.value, directive.end, -1);

    directive.comment = document.getElementById('details_comment').value;
    directive.author = document.getElementById('details_author').value;

    if (saveAction) {
        saveAction(directive);
    }

    saveChanges(directive);
});
