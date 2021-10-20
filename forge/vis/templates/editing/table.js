let displayEditDirectiveDetails = (originalDirective, onsave) => {};
let selectEditDirectiveAction = (directive, validityLock) => {};

$(document).ready(function() {
    const directivesTable = document.getElementById('edit_directives');

    function sortTable(column, reverse) {
        let rows = Array.from(directivesTable.rows);
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
        rows.forEach(row => directivesTable.appendChild(row));
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

    function directiveFromRow(tr) { return tr._editDirective; }
    function setRowDirective(tr, directive) { tr._editDirective = directive; }

    const showAllDirectiveTypes = document.getElementById('show_type');
    const showModifiedTime = document.getElementById('show_modified_time');
    const showDeletedDirectives = document.getElementById('show_deleted');
    const exportCSV = document.getElementById('export_csv');
    const exportJSON = document.getElementById('export_json');

    function setHeaderVisibility() {
        if (showAllDirectiveTypes.checked) {
            $('tr.edit-header th:nth-child(2)').removeClass('hidden');
        } else {
            $('tr.edit-header th:nth-child(2)').addClass('hidden');
        }
        if (showModifiedTime.checked) {
            $('tr.edit-header th:nth-child(4)').removeClass('hidden');
        } else {
            $('tr.edit-header th:nth-child(4)').addClass('hidden');
        }
    }

    function setExportTimes(a) {
        a.href = $(a).attr('base') + "?start=" + TimeSelect.start_ms + '&end=' + TimeSelect.end_ms;
        if (showDeletedDirectives.checked) {
            a.href = a.href + '&deleted';
        }
        if (showAllDirectiveTypes.checked) {
            a.href = a.href + '&alltypes';
        }
    }

    function isDirectiveHidden(directive) {
        if (!showDeletedDirectives.checked) {
            if (directive.deleted) {
                return true;
            }
        }
        if (!showAllDirectiveTypes.checked) {
            if (directive.other_type) {
                return true;
            }
        }
        return false;
    }
    function setRowVisibility(tr) {
        if (showAllDirectiveTypes.checked) {
            tr.children[2].classList.remove('hidden');
        } else {
            tr.children[2].classList.add('hidden');
        }
        if (showModifiedTime.checked) {
            tr.children[4].classList.remove('hidden');
        } else {
            tr.children[4].classList.add('hidden');
        }

        if (isDirectiveHidden(directiveFromRow(tr))) {
            tr.classList.add('hidden');
        } else {
            tr.classList.remove('hidden');
        }
    }

    function setAllRowsVisibility() {
        for (let i=0; i<directivesTable.rows.length; i++) {
            setRowVisibility(directivesTable.rows[i]);
        }
    }
    $('#show_modified_time').click(function(event) {
        setHeaderVisibility();
        setAllRowsVisibility();

        if (this.checked) {
            localStorage.setItem('forge-settings-edit-show-modified', '1');
        } else {
            localStorage.removeItem('forge-settings-edit-show-modified');
        }
    });
    if (localStorage.getItem('forge-settings-edit-show-modified')) {
        document.getElementById('show_modified_time').checked = true;
    }
    $(showAllDirectiveTypes).click(function(event) {
        setHeaderVisibility();
        setAllRowsVisibility();
        setExportTimes(exportCSV);
        setExportTimes(exportJSON);

        if (this.checked) {
            localStorage.setItem('forge-settings-edit-show-type', '1');
        } else {
            localStorage.removeItem('forge-settings-edit-show-type');
        }
    });
    if (localStorage.getItem('forge-settings-edit-show-type')) {
        showAllDirectiveTypes.checked = true;
    }
    $(showDeletedDirectives).click(function(event) {
        setHeaderVisibility();
        setAllRowsVisibility();
        setExportTimes(exportCSV);
        setExportTimes(exportJSON);

        if (this.checked) {
            localStorage.setItem('forge-settings-edit-show-deleted', '1');
        } else {
            localStorage.removeItem('forge-settings-edit-show-deleted');
        }
    });
    if (localStorage.getItem('forge-settings-edit-show-deleted')) {
        showDeletedDirectives.checked = true;
    }
    setHeaderVisibility();
    setExportTimes(exportCSV);
    setExportTimes(exportJSON);


    function getDirectiveSummary(directive) {
        if (directive.selection) {
            const summaryItems = new Set();
            directive.selection.forEach((selection) => {
                EditDirectiveAvailable.findMatching(selection).forEach((available) => {
                    summaryItems.add(available.summaryText());
                });
            });


            const sorted = Array.from(summaryItems);
            sorted.sort();
            let result = sorted.join(" ");
            if (result.length > 16) {
                result = result.slice(0, 16);
                result += "…";
            }
            return result;
        } else if (directive.instrument) {
            return directive.instrument;
        }

        return "";
    }

    function rowSelected(selected) {
        for (let i=0; i<directivesTable.rows.length; i++) {
            const tr = directivesTable.rows[i];
            if (tr === this) {
                continue;
            }
            tr.classList.remove('selected');
        }
        selected.classList.add('selected');
        $('.requires-selected').removeAttr('disabled');

        const directive = directiveFromRow(selected);
        PlotInteraction.notifyDirectiveSelected(directive);
        if (directive.deleted) {
            $('#remove_directive').text("Restore");
        } else {
            $('#remove_directive').text("Remove");
        }
    }
    function clearDirectiveSelection() {
        for (let i=0; i<directivesTable.rows.length; i++) {
            directivesTable.rows[i].classList.remove('selected');
        }
        $('.requires-selected').attr('disabled', 'disabled');
        PlotInteraction.notifyDirectiveSelected(null);
    }

    const actionDisplayName = new Map();
    // {% for code, action in actions.items() %}
    actionDisplayName['{{ code }}'] = '{{ action.display }}';
    // {% endfor %}

    function updateRow(tr, directive) {
        setRowDirective(tr, directive);

        tr.children[0].textContent = TimeParse.toDisplayTime(directive.start_epoch_ms, ' ', ' ');
        tr.children[1].textContent = TimeParse.toDisplayTime(directive.end_epoch_ms, ' ', ' ');
        tr.children[2].textContent = directive.type;
        tr.children[3].textContent = directive.author;
        tr.children[4].textContent = TimeParse.toDisplayTime(directive.modified_epoch_ms, ' ', ' ');
        tr.children[5].textContent = actionDisplayName[directive.action];
        tr.children[6].textContent = getDirectiveSummary(directive);
        tr.children[7].textContent = directive.comment;

        if (directive.deleted) {
            tr.classList.add('edit-deleted');
        } else {
            tr.classList.remove('edit-deleted');
        }

        if (directive.other_type) {
            tr.classList.add('edit-other-type');
        } else {
            tr.classList.remove('edit-other-type');
        }

        setRowVisibility(tr);
    }
    function addDirectiveRow() {
        const tr = directivesTable.insertRow();
        for (let i=0; i<8; i++) {
            tr.insertCell();
        }
        $(tr).click(function(event) {
            rowSelected(this);
        });
        return tr;
    }
    function addEditDirectiveToTable(directive) {
        const tr = addDirectiveRow();
        updateRow(tr, directive);
        return tr;
    }

    function saveSelectedDirective(directive) {
        const tr = $('#edit_directives tr.selected').get(0);
        if (tr === undefined) {
            return;
        }

        showLoading();
        $.post("{{ request.url_for('edit_save', station=station, mode_name=mode_name) }}", JSON.stringify(directive), function(saved) {
            hideLoading();
            PlotInteraction.notifyDirectivesChanged();
            updateRow(tr, saved);
            if (saved.deleted && !showDeletedDirectives.checked) {
                clearDirectiveSelection();
            } else {
                rowSelected(tr);
            }
        }).fail(function() {
            hideLoading();
            window.alert("Error saving edit directive, changes lost");
        });
    }
    function saveNewDirective(directive) {
        showLoading();
        $.post("{{ request.url_for('edit_save', station=station, mode_name=mode_name) }}", JSON.stringify(directive), function(saved) {
            hideLoading();
            PlotInteraction.notifyDirectivesChanged();
            const tr = addEditDirectiveToTable(saved);
            rowSelected(tr);
        }).fail(function() {
            hideLoading();
            window.alert("Error adding edit directive, changes lost");
        });
    }

    $('#add_directive').click(function(event) {
        showModal("{{ request.url_for('edit_details', station=station) }}", () => {
            displayEditDirectiveDetails(undefined, saveNewDirective);
        });
    });
    $('#remove_directive').click(function(event) {
        const tr = $('#edit_directives tr.selected').get(0);
        if (tr === undefined) {
            return;
        }
        const directive = JSON.parse(JSON.stringify(directiveFromRow(tr)));
        directive.deleted = !directive.deleted;
        saveSelectedDirective(directive);
    });
    $('#modify_directive').click(function(event) {
        const tr = $('#edit_directives tr.selected').get(0);
        if (tr === undefined) {
            return;
        }
        showModal("{{ request.url_for('edit_details', station=station) }}", () => {
            displayEditDirectiveDetails(directiveFromRow(tr), saveSelectedDirective);
        });
    });
    $('#duplicate_directive').click(function(event) {
        const original = $('#edit_directives tr.selected').get(0);
        if (original === undefined) {
            return;
        }
        const directive = JSON.parse(JSON.stringify(directiveFromRow(original)));
        delete directive.deleted;
        delete directive._id;
        showModal("{{ request.url_for('edit_details', station=station) }}", () => {
            displayEditDirectiveDetails(directive, saveNewDirective);
        });
    });

    const DirectiveStream = class extends DataSocket.Stream {
        constructor() {
            super('{{ mode_name }}-directives');
            showLoading();
        }

        startOfData() {
            while (directivesTable.rows.length > 0) {
                directivesTable.deleteRow();
            }
            $('.requires-selected').attr('disabled', 'disabled');
        }

        endOfData() {
            hideLoading();
        }

        incomingDataContent(content) {
            addEditDirectiveToTable(content);
        }
    };
    (new DirectiveStream()).beginStream();

    EditDirectiveAvailable.ready(() => {
        for (let i=0; i<directivesTable.rows.length; i++) {
            const tr = directivesTable.rows[i];
            tr.children[6].textContent = getDirectiveSummary(directiveFromRow(tr));
        }
    });
});
