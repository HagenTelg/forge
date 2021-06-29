Selection.target = [];


function updateTarget() {
    Selection.target.length = 0;

    for (let i=0; i<Selection.list.rows.length; i++) {
        const tr = Selection.list.rows[i];
        if (!tr.classList.contains('selected')) {
            continue;
        }

        if (tr.originSelection !== undefined) {
            Selection.target.push(tr.originSelection);
        } else {
            Selection.target.push(tr.originAvailable.selection());
        }
    }
}

function rowSelected(tr) {
    tr.classList.toggle('selected');
    updateTarget();
}


function sortList() {
    let rows = Array.from(Selection.list.rows);
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
    rows.forEach(row => Selection.list.appendChild(row));
}

function addListRow() {
    const tr = Selection.list.insertRow();
    for (let i=0; i<1; i++) {
        tr.insertCell();
    }
    $(tr).click(function(event) {
        rowSelected(this);
    });
    return tr;
}
function configureRowAvailable(tr, available) {
    tr.originAvailable = available;
    tr.children[0].textContent = available.selectionText();
}
function configureRowSelection(tr, selection) {
    tr.originSelection = selection;
    if (selection.type === 'variable') {
        tr.children[0].textContent = selection.variable;
    } else {
        tr.children[0].textContent = "UNKNOWN";
    }
}
function availableMatchesRow(tr, available) {
    if (tr.originAvailable !== undefined) {
        return false;
    }
    return available.matchesExactly(tr.originSelection);
}
function selectionMatchesRow(tr, selection) {
    if (tr.originAvailable === undefined) {
        return false;
    }
    return tr.originAvailable.matchesExactly(selection);
}


function createSelectionMenuEntry(text, cb) {
    const li = document.createElement('li');

    const action = document.createElement('button');
    action.textContent = text;
    $(action).click(cb);
    li.appendChild(action);

    return li;
}

class SelectionShortcut {
    constructor(display, patterns) {
        this.display = display;
        this.patterns = [];
        for (const pattern of patterns) {
            this.patterns.push(new RegExp(pattern));
        }
    }

    matches(available) {
        if (!available.variable) {
            return false;
        }

        for (let i=0; i<this.patterns.length; i++) {
            if (this.patterns[i].exec(available.variable)) {
                return true;
            }
        }
        return false;
    }

    matchesRow(tr) {
        if (tr.originAvailable === undefined) {
            return false;
        }
        return this.matches(tr.originAvailable);
    }

    activate() {
        let allAlreadySelected = true;
        for (let i=0; i<Selection.list.rows.length; i++) {
            const tr = Selection.list.rows[i];
            if (!this.matchesRow(tr)) {
                continue;
            }

            if (!tr.classList.contains('selected')) {
                tr.classList.add('selected');
                allAlreadySelected = false;
            }
        }
        if (allAlreadySelected) {
            for (let i=0; i<Selection.list.rows.length; i++) {
                const tr = Selection.list.rows[i];
                if (!this.matchesRow(tr)) {
                    continue;
                }

                tr.classList.remove('selected');
            }
        }
        updateTarget();
    }
}
// {% for selection in selections %}
(function() {
    const selection = new SelectionShortcut('{{ selection.display }}', [
        // {% for pattern in selection.patterns %}
        '{{ pattern }}',
        // {% endfor %}
    ]);

    const li = createSelectionMenuEntry(selection.display, function(event) {
        selection.activate();
    });
    li.style.display = 'none';
    Selection.shortcut_menu.appendChild(li);


    EditDirectiveAvailable.ready(() => {
        for (let i=0; i<EditDirectiveAvailable.available.length; i++) {
            const available = EditDirectiveAvailable.available[i];
            if (!selection.matches(available)) {
                continue;
            }
            li.style.display = 'block';
            return;
        }
    });
})();
// {% endfor %}

class InstrumentSelectionShortcut {
    constructor(display, patterns) {
        this.display = display;
        this.patterns = [];
        for (const pattern of patterns) {
            this.patterns.push(new RegExp(patterns));
        }
        this.instrument = undefined;
        this.require = undefined;
    }

    matchesInitial(instrument, variables) {
        if (this.instrument !== undefined) {
            if (!this.instrument.exec(instrument)) {
                return false;
            }
        }

        if (this.require !== undefined) {
            let hit = false;
            for (let variable of variables) {
                if (this.require.exec(variable)) {
                    hit = true;
                    break;
                }
            }
            if (!hit) {
                return false;
            }
        }

        for (let variable of variables) {
            for (var i=0; i<this.patterns.length; i++) {
                if (this.patterns[i].exec(variable)) {
                    return true;
                }
            }
        }
        return false;
    }

    matchesAvailable(instrument, available) {
        if (!available.variable) {
            return false;
        }

        const parts = available.variable.split('_', 2);
        if (parts.length !== 2) {
            return false;
        }

        const variable = parts[0];
        if (parts[1] !== instrument) {
            return false;
        }

        if (this.instrument !== undefined) {
            if (!this.instrument.exec(instrument)) {
                return false;
            }
        }

        for (let i=0; i<this.patterns.length; i++) {
            if (this.patterns[i].exec(variable)) {
                return true;
            }
        }
        return false;
    }

    matchesRow(instrument, tr) {
        if (tr.originAvailable === undefined) {
            return false;
        }
        return this.matchesAvailable(instrument, tr.originAvailable);
    }

    activate(instrument) {
        let allAlreadySelected = true;
        for (let i=0; i<Selection.list.rows.length; i++) {
            const tr = Selection.list.rows[i];
            if (!this.matchesRow(instrument, tr)) {
                continue;
            }

            if (!tr.classList.contains('selected')) {
                tr.classList.add('selected');
                allAlreadySelected = false;
            }
        }
        if (allAlreadySelected) {
            for (let i=0; i<Selection.list.rows.length; i++) {
                const tr = Selection.list.rows[i];
                if (!this.matchesRow(instrument, tr)) {
                    continue;
                }

                tr.classList.remove('selected');
            }
        }
        updateTarget();
    }
}
const instrumentSelections = [
    // {% for selection in instrument_selections %}
    (function() {
        const selection = new InstrumentSelectionShortcut('{{ selection.display }}', [
            // {% for pattern in selection.patterns %}
            '{{ pattern }}',
            // {% endfor %}
        ]);
        // {% if selection.instrument %}
        selection.instrument = new RegExp('{{ selection.instrument }}');
        // {% endif %}
        // {% if selection.require %}
        selection.require = new RegExp('{{ selection.require }}');
        // {% endif %}

        return selection;
    })(),
    // {% endfor %}
];

EditDirectiveAvailable.ready(() => {
    const instrumentVariables = new Map();
    EditDirectiveAvailable.available.forEach((available) => {
        if (available.variable) {
            const parts = available.variable.split('_', 2);
            if (parts.length !== 2) {
                return;
            }
            const variable = parts[0];
            const instrument = parts[1];
            if (!instrumentVariables.has(instrument)) {
                instrumentVariables.set(instrument, new Set());
            }
            instrumentVariables.get(instrument).add(variable)
        }
    });

    function constructRightMenu(text) {
        const li = document.createElement('li');

        const button = document.createElement('button');
        button.classList.add('details-menu-right');
        button.textContent = text;
        li.appendChild(button);
        const mdi = document.createElement('i');
        mdi.classList.add('mdi', 'mdi-menu-right');
        button.appendChild(mdi);

        const ul = document.createElement('ul');
        ul.classList.add('details-menu-content');
        li.appendChild(ul);

        return [li, ul]
    }

    instrumentVariables.forEach((variables, instrument) => {
        let instrumentMenu = undefined;
        instrumentSelections.forEach((selection) => {
            if (!selection.matchesInitial(instrument, variables)) {
                return;
            }

            if (instrumentMenu === undefined) {
                const menu = constructRightMenu(instrument);
                Selection.instrument_menu.appendChild(menu[0]);
                instrumentMenu = menu[1];
            }

            const li = createSelectionMenuEntry(selection.display, function(event) {
                selection.activate(instrument);
            });
            instrumentMenu.appendChild(li);
        });
    });

    let anySelectionsShown = false;
    for (let i=1; i<Selection.shortcut_menu.children.length; i++) {
        const li = Selection.shortcut_menu.children[i];
        if (li.style.display === 'none') {
            continue;
        }
        anySelectionsShown = true;
        break;
    }
    if (Selection.instrument_menu.children.length === 0) {
        if (Selection.shortcut_menu.children.length === 1 || !anySelectionsShown) {
            Selection.shortcut_menu.parentNode.style.display = 'none';
            return;
        }
        Selection.instrument_menu.parentNode.display = 'none';
    } else if (!anySelectionsShown) {
        Selection.instrument_menu.parentNode.display = 'none';
        Array.from(Selection.instrument_menu.children).forEach((li) => {
            Selection.shortcut_menu.appendChild(li);
        });
    }
});

EditDirectiveAvailable.ready(() => {
    let unclaimedAvailable = [];

    EditDirectiveAvailable.available.forEach((available) => {
        for (let i=0; i<Selection.list.rows.length; i++) {
            const tr = Selection.list.rows[i];
            if (availableMatchesRow(tr, available)) {
                tr.originAvailable = available;
                return;
            }
        }
        unclaimedAvailable.push(available);
    });
    unclaimedAvailable.forEach((available) => {
        const tr = addListRow();
        configureRowAvailable(tr, available);
    });

    sortList();
});

Selection.configure = function(directive) {
    for (let i=0; i<Selection.list.rows.length; i++) {
        Selection.list.rows[i].classList.remove('selected');
    }
    if (!directive.selection) {
        directive.selection = [];
    }
    Selection.target = directive.selection;

    let unclaimedSelections = [];
    Selection.target.forEach((selection) => {
        for (let i=0; i<Selection.list.rows.length; i++) {
            const tr = Selection.list.rows[i];
            if (selectionMatchesRow(tr, selection)) {
                tr.originSelection = selection;
                tr.classList.add('selected');
                return;
            }
        }
        unclaimedSelections.push(selection);
    });
    unclaimedSelections.forEach((selection) => {
        const tr = addListRow();
        configureRowSelection(tr, selection);
        tr.classList.add('selected');
    });

    sortList();
};
