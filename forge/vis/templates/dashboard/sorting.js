const Sorting = {};
(function () {
    Sorting.visibleEntries = [];

    let sortOrder = [1, 2, 3, 4, 5];

    const existingSortOrder = localStorage.getItem('forge-dashboard-sort')
    if (existingSortOrder) {
        sortOrder.length = 0;
        for (const index of existingSortOrder.split(' ')) {
            sortOrder.push(index * 1);
        }
        sortOrder.push(5);
    }

    function saveSortOrder() {
        localStorage.setItem('forge-dashboard-sort', sortOrder.join(' '));
    }

    function applySort() {
        Sorting.visibleEntries.sort((a, b) => {
            const dataA = a.sortData;
            const dataB = b.sortData;
            for (let i=0; i<sortOrder.length; i++) {
                let orderIndex = sortOrder[i];
                let reverse = false;
                if (orderIndex < 0) {
                    reverse = true;
                    orderIndex = -orderIndex;
                }
                orderIndex -= 1;

                const ca = dataA[orderIndex];
                const cb = dataB[orderIndex];
                if (ca < cb) {
                    return reverse ? 1 : -1;
                } else if (ca > cb) {
                    return reverse ? -1 : 1;
                }
            }
            return 0;
        });
    }

    const entryTableData = document.getElementById('dashboard_entries');

    let deferredTableTimer = undefined;

    Sorting.updateTable = function() {
        if (deferredTableTimer) {
            clearTimeout(deferredTableTimer);
            deferredTableTimer = undefined;
        }

        while (entryTableData.firstChild) {
            entryTableData.removeChild(entryTableData.firstChild);
        }

        applySort();

        for (const entry of Sorting.visibleEntries) {
            entryTableData.appendChild(entry.row_header);
            entryTableData.appendChild(entry.row_details);
        }
    }

    function runDeferUpdateTable() {
        deferredTableTimer = undefined;
        Sorting.updateTable();
    }

    Sorting.deferUpdateTable = function() {
        if (deferredTableTimer) {
            return;
        }
        deferredTableTimer = setTimeout(runDeferUpdateTable);
    }

    $('tr.entry-header .sort-down').click(function(event) {
        event.preventDefault();

        const index = $(this).attr('sort') * 1;

        let hit = sortOrder.indexOf(index);
        if (hit !== -1) {
            sortOrder.splice(hit, 1);
        }
        hit = sortOrder.indexOf(-index);
        if (hit !== -1) {
            sortOrder.splice(hit, 1);
        }

        sortOrder.unshift(index);
        saveSortOrder();
        Sorting.deferUpdateTable();
    });
    $('tr.entry-header .sort-up').click(function(event) {
        event.preventDefault();

        const index = $(this).attr('sort') * 1;

        let hit = sortOrder.indexOf(index);
        if (hit !== -1) {
            sortOrder.splice(hit, 1);
        }
        hit = sortOrder.indexOf(-index);
        if (hit !== -1) {
            sortOrder.splice(hit, 1);
        }

        sortOrder.unshift(-index);
        saveSortOrder();
        Sorting.deferUpdateTable();
    });
})();