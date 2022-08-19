let directive = {};
let saveChanges = (directive) => {};
let saveAction = undefined;
let saveCondition = undefined;

const startTimeEntry = document.getElementById("details-input-start-time");
const startTimeDisplay = document.getElementById("details-parsed-start-time");
const endTimeEntry = document.getElementById("details-input-end-time");
const endTimeDisplay = document.getElementById("details-parsed-end-time");
const detailsTimeTable = document.getElementById("details-time-bounds");
const acceptButton = document.getElementById('details_accept');

const invalidLocks = new Set();
const actionInvalidLocks = new Set();
const conditionInvalidLocks = new Set();
function addInvalidLock(key) {
    invalidLocks.add(key);
    acceptButton.classList.add('invalid');
}
function isValid() {
    return !invalidLocks.size && !actionInvalidLocks.size && !conditionInvalidLocks.size;
}
function removeInvalidLock(key) {
    invalidLocks.delete(key);
    if (isValid()) {
        acceptButton.classList.remove('invalid');
    }
}
function clearActionInvalidLocks() {
    actionInvalidLocks.clear();
    if (isValid()) {
        acceptButton.classList.remove('invalid');
    }
}
function clearConditionInvalidLocks() {
    conditionInvalidLocks.clear();
    if (isValid()) {
        acceptButton.classList.remove('invalid');
    }
}

let actionLockController = {
    addInvalidLock: function(key) {
        actionInvalidLocks.add(key);
        acceptButton.classList.add('invalid');
    },
    removeInvalidLock: function(key) {
        actionInvalidLocks.delete(key);
        if (isValid()) {
            acceptButton.classList.remove('invalid');
        }
    },
}
let conditionLockController = {
    addInvalidLock: function(key) {
        conditionInvalidLocks.add(key);
        acceptButton.classList.add('invalid');
    },
    removeInvalidLock: function(key) {
        conditionInvalidLocks.delete(key);
        if (isValid()) {
            acceptButton.classList.remove('invalid');
        }
    },
}

function startTimeEdited() {
    const parsedEnd = TimeParse.parseTime(endTimeEntry.value, PlotInteraction.start_ms, 1);
    const parsedStart = TimeParse.parseTime(startTimeEntry.value, parsedEnd, -1);

    if (parsedStart !== null && !parsedStart) {
        startTimeEntry.classList.add('invalid');
        startTimeDisplay.classList.add('invalid');

        startTimeDisplay.textContent = "ERROR";
        addInvalidLock('start_time');

        removeInvalidLock('invalid_bounds');
        detailsTimeTable.classList.remove('invalid');
        return;
    }

    removeInvalidLock('start_time');
    startTimeEntry.classList.remove('invalid');
    startTimeDisplay.classList.remove('invalid');
    startTimeDisplay.textContent = TimeParse.toDisplayTime(parsedStart);
    directive.start_epoch_ms = parsedStart;
    PlotInteraction.notifyDirectiveSelected(directive);

    if (parsedStart !== null && parsedEnd !== null && parsedStart >= parsedEnd) {
        detailsTimeTable.classList.add('invalid');
        addInvalidLock('invalid_bounds');
        return
    }
    removeInvalidLock('invalid_bounds');
    detailsTimeTable.classList.remove('invalid');
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

        removeInvalidLock('invalid_bounds');
        detailsTimeTable.classList.remove('invalid');
        return;
    }

    if (parsedStart !== null && parsedEnd !== null && parsedStart >= parsedEnd) {
        detailsTimeTable.classList.add('invalid');
        addInvalidLock('invalid_bounds');
        return
    }
    removeInvalidLock('invalid_bounds');
    detailsTimeTable.classList.remove('invalid');

    removeInvalidLock('end_time');
    removeInvalidLock('invalid_bounds');
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

    removeInvalidLock('invalid_bounds');
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
    $('#details_condition').val(directive.condition.type).change();
    populateHistory();
}

displayEditDirectiveDetails = function(originalDirective, onsave) {
    if (originalDirective) {
        directive = JSON.parse(JSON.stringify(originalDirective));
    } else {
        directive = {
            start_epoch_ms: PlotInteraction.start_ms,
            end_epoch_ms: PlotInteraction.end_ms,
            author: "{{ request.user.initials|e }}",
            action: 'invalidate',
            condition: { type: 'none' },
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
    directive.action = this.value;
    clearActionInvalidLocks();

    const selectedOption = this.options[this.selectedIndex];
    const editor = $(selectedOption).attr('editor');
    $.ajax(editor).done(function(responseText) {
        $('#details_action_content').html(responseText).ready(() => {
            saveAction = selectEditDirectiveAction(directive, actionLockController);
        });
    });
});
$('#details_condition').change(function(event) {
    $('.details-condition-description').removeClass('active');
    $('.details-condition-description[code=' + this.value + ']').addClass('active');
    directive.condition.type = this.value;
    clearConditionInvalidLocks();

    const selectedOption = this.options[this.selectedIndex];
    const editor = $(selectedOption).attr('editor');
    $.ajax(editor).done(function(responseText) {
        $('#details_condition_content').html(responseText).ready(() => {
            saveCondition = selectEditDirectiveCondition(directive, conditionLockController);
        });
    });
});

$(acceptButton).click(function(event) {
    if (invalidLocks.size) {
        event.preventDefault();
        return;
    }

    const parsedStart = TimeParse.parseTime(startTimeEntry.value, PlotInteraction.end_ms, -1);
    directive.end = TimeParse.parseTime(endTimeEntry.value, parsedStart, 1);
    directive.start = TimeParse.parseTime(startTimeEntry.value, directive.end, -1);

    directive.comment = document.getElementById('details_comment').value;
    directive.author = document.getElementById('details_author').value;

    if (saveAction) {
        if (!saveAction(directive)) {
            event.preventDefault();
            return;
        }
    }
    if (saveCondition) {
        if (!saveCondition(directive)) {
            event.preventDefault();
            return;
        }
    }

    hideModal();
    saveChanges(directive);
});
