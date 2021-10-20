Instrument.apply = function(instrument) {};
Instrument._selectionFilter = [];
Instrument.lockController = {
    addInvalidLock: function(key) { },
    removeInvalidLock: function(key) { },
};


function rowSelected(tr) {
    for (let i=0; i<Instrument.list.rows.length; i++) {
        const tr = Instrument.list.rows[i];
        if (tr === this) {
            continue;
        }
        tr.classList.remove('selected');
    }
    tr.classList.add('selected');

    Instrument.apply(tr.instrument);
    Instrument.lockController.removeInvalidLock(Instrument.lockKey);
}

function sortList() {
    let rows = Array.from(Instrument.list.rows);
    rows.sort((a, b) => {
        const ca = a.children[0].textContent;
        const cb = b.children[0].textContent;
        if (ca < cb) {
            return -1;
        } else if (ca > cb) {
            return 1;
        }
        return 0;
    });
    rows.forEach(row => Instrument.list.appendChild(row));
}

function addListRow() {
    const tr = Instrument.list.insertRow();
    for (let i=0; i<1; i++) {
        tr.insertCell();
    }
    $(tr).click(function(event) {
        rowSelected(this);
    });
    tr.originAvailable = [];
    return tr;
}
function configureRowInstrument(tr, instrument) {
    tr.instrument = instrument;
    tr.children[0].textContent = instrument;
}
function instrumentMatchesRow(tr, instrument) {
    return tr.instrument === instrument;
}

Instrument.isValid = function() {
    for (let i=0; i<Instrument.list.rows.length; i++) {
        const tr = Instrument.list.rows[i];
        if (tr.classList.contains('hidden')) {
            continue;
        }
        if (tr.classList.contains('selected')) {
            return true;
        }
    }
    return false;
}

function filterVisible() {
    function isVisible(tr) {
        if (tr.originAvailable.length === 0) {
            return true;
        }

        function matchesSelection(selection) {
            for (let i=0; i<tr.originAvailable.length; i++) {
                const available = tr.originAvailable[i];
                if (available.matches(selection)) {
                    return true;
                }
            }
            return false;
        }

        function matchesAnySelection() {
            if (Instrument._selectionFilter.length <= 0) {
                return true;
            }
            for (let i=0; i<Instrument._selectionFilter.length; i++) {
                const selection = Instrument._selectionFilter[i];
                if (matchesSelection(selection)) {
                    return true;
                }
            }
            return false;
        }

        return matchesAnySelection();
    }

    let singleVisibleRow = undefined;
    let countVisible = 0;
    for (let i=0; i<Instrument.list.rows.length; i++) {
        const tr = Instrument.list.rows[i];
        if (!tr.classList.contains('selected') && !isVisible(tr)) {
            tr.classList.add('hidden');
        } else {
            tr.classList.remove('hidden');
            countVisible++;
            singleVisibleRow = tr;
        }
    }

    if (singleVisibleRow && countVisible === 1) {
        rowSelected(singleVisibleRow);
    } else if (countVisible === 0) {
        Instrument.lockController.addInvalidLock(Instrument.lockKey);
    }
}

Instrument.setSelectionFilter = function(...selections) {
    Instrument._selectionFilter = selections;
    filterVisible();
}

EditDirectiveAvailable.ready(() => {
    const unclaimedAvailable = new Map();
    EditDirectiveAvailable.available.forEach((available) => {
        const availableInstrument = available.instrument();
        if (!availableInstrument) {
            return;
        }

        for (let i=0; i<Instrument.list.rows.length; i++) {
            const tr = Instrument.list.rows[i];
            if (instrumentMatchesRow(tr, availableInstrument)) {
                tr.originAvailable.push(available);
                return;
            }
        }

        let target = unclaimedAvailable.get(availableInstrument);
        if (!target) {
            target = [];
            unclaimedAvailable.set(availableInstrument, target);
        }
        target.push(available);
    });
    unclaimedAvailable.forEach((originAvailable, availableInstrument) => {
        const tr = addListRow();
        tr.originAvailable = originAvailable;
        configureRowInstrument(tr, availableInstrument);
    });

    sortList();
    filterVisible();
});

Instrument.configure = function(directive, field) {
    if (!field) {
        field = 'instrument';
    }
    let original = directive[field];
    if (original === '') {
        original = undefined;
    }
    Instrument.apply = function(instrument) { directive[field] = instrument; }

    if (original) {
        for (let i=0; i<Instrument.list.rows.length; i++) {
            const tr = Instrument.list.rows[i];
            if (tr === this) {
                continue;
            }
            tr.classList.remove('selected');
        }
        for (let i=0; i<Instrument.list.rows.length; i++) {
            const tr = Instrument.list.rows[i];
            if (instrumentMatchesRow(tr, original)) {
                tr.classList.add('selected');
                tr.classList.remove('hidden');
                Instrument.lockController.removeInvalidLock(Instrument.lockKey);
                return;
            }
        }

        const tr = addListRow();
        configureRowInstrument(tr, original);
        tr.classList.add('selected');
    }

    if (!Instrument.isValid()) {
        Instrument.lockController.addInvalidLock(Instrument.lockKey);
    } else {
        Instrument.lockController.removeInvalidLock(Instrument.lockKey);
    }
};