const selectInterval = document.getElementById('periodic_interval');
const selectDivision = document.getElementById('periodic_division');
const pointList = document.getElementById('details_condition_periodic_list');
let conditionTarget = {};

const intervals = {
    hour: {
        duration: 60 * 60,
        divisions: [ 'minute' ],
    },
    day: {
        duration: 24 * 60 * 60,
        divisions: [ 'hour', 'minute' ],
    },
};
const divisions = {
    minute: {
        interval: 60,
    },
    hour: {
        interval: 60 * 60,
    },
};

function updateSelectedPoints() {
    conditionTarget.points.length = 0;

    for (let i=0; i<pointList.rows.length; i++) {
        const tr = pointList.rows[i];
        if (!tr.classList.contains('selected')) {
            continue;
        }

        conditionTarget.points.push(i);
    }
}

function rowSelected(tr) {
    tr.classList.toggle('selected');
    updateSelectedPoints();
}


function setVisibleDivisions() {
    const interval = intervals[conditionTarget.interval];
    $('#periodic_division > option').each(function() {
        if (interval.divisions.indexOf(this.value) < 0) {
            this.classList.add('hidden');
        } else {
            this.classList.remove('hidden');
        }
    });
}
function populatePointList() {
    while (pointList.firstChild) {
        pointList.removeChild(pointList.firstChild);
    }

    function addPoint() {
        const tr = pointList.insertRow();
        for (let i=0; i<1; i++) {
            tr.insertCell();
        }
        $(tr).click(function(event) {
            rowSelected(this);
        });
        return tr;
    }

    function formatTime(timeOffset) {
        return Math.floor(timeOffset / (60 * 60)).toFixed(0).padStart(2, '0') + ":" +
            Math.floor((timeOffset % (60 * 60)) / 60).toFixed(0).padStart(2, '0') + ":" +
            Math.floor(timeOffset % 60).toFixed(0).padStart(2, '0');
    }

    const totalDuration = intervals[conditionTarget.interval].duration;
    const pointSpacing = divisions[conditionTarget.division].interval;
    for (let timeOffset = 0, rowIndex = 0, selectedPointsIndex = 0;
            timeOffset < totalDuration; timeOffset += pointSpacing, rowIndex++) {
        const tr = addPoint();
        tr.children[0].textContent = formatTime(timeOffset);

        while (selectedPointsIndex < conditionTarget.points.length) {
            if (conditionTarget.points[selectedPointsIndex] >= rowIndex) {
                break;
            }
            ++selectedPointsIndex;
        }

        if (selectedPointsIndex < conditionTarget.points.length &&
                rowIndex === conditionTarget.points[selectedPointsIndex]) {
            tr.classList.add('selected');
        }
    }
}


$(selectInterval).change(function(event) {
    if (conditionTarget.interval === this.value) {
        return;
    }
    conditionTarget.points.length = 0;
    conditionTarget.interval = this.value;

    const interval = intervals[conditionTarget.interval];

    if (!conditionTarget.division || !divisions[conditionTarget.division] ||
            interval.divisions.indexOf(conditionTarget.division) < 0) {
        conditionTarget.division = interval.divisions[0];
        selectDivision.value = conditionTarget.division;
    }

    setVisibleDivisions();
    populatePointList();
});
$(selectDivision).change(function(event) {
    if (conditionTarget.division === this.value) {
        return;
    }
    conditionTarget.points.length = 0;
    conditionTarget.division = this.value;
    populatePointList();
});


selectEditDirectiveCondition = function(directive, lockController) {
    conditionTarget = directive.condition;

    if (!conditionTarget.interval || !intervals[conditionTarget.interval]) {
        conditionTarget.interval = 'hour';
    }
    const interval = intervals[conditionTarget.interval];
    selectInterval.value = conditionTarget.interval;

    if (!conditionTarget.division || !divisions[conditionTarget.division] ||
            interval.divisions.indexOf(conditionTarget.division) < 0) {
        conditionTarget.division = interval.divisions[0];
    }
    selectDivision.value = conditionTarget.division;

    if (!conditionTarget.points) {
        conditionTarget.points = [];
    }

    setVisibleDivisions();
    populatePointList();
}