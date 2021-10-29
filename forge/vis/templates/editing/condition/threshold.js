const variableLabel = document.getElementById('edit_condition_threshold_value');
ConditionSelection.changed = function(selected) {
    if (selected.length === 1 && selected[0].type === 'variable') {
        variableLabel.textContent = selected[0].variable;
        return;
    }
    variableLabel.textContent = 'x';
}

const lowerBoundInput = document.getElementById('edit_condition_threshold_lower');
const upperBoundInput = document.getElementById('edit_condition_threshold_upper');
let conditionTarget = {};

function isValidRange() {
    if (!isFinite(conditionTarget.lower)) {
        return true;
    }
    if (!isFinite(conditionTarget.upper)) {
        return true;
    }
    return conditionTarget.lower < conditionTarget.upper;
}

selectEditDirectiveCondition = function(directive, lockController) {
    conditionTarget = directive.condition;

    ConditionSelection.configure(conditionTarget);
    if (isFinite(conditionTarget.lower)) {
        lowerBoundInput.value = conditionTarget.lower.toString();
    } else {
        lowerBoundInput.value = '';
    }
    if (isFinite(conditionTarget.upper)) {
        upperBoundInput.value = conditionTarget.upper.toString();
    } else {
        upperBoundInput.value = '';
    }

    if (!isValidRange()) {
        lowerBoundInput.classList.add('invalid');
        upperBoundInput.classList.add('invalid');
    } else {
        lowerBoundInput.classList.remove('invalid');
        upperBoundInput.classList.remove('invalid');
    }
};

function toLimitValue(value) {
    if (value.match(/^\s*(undef|([+-]?inf)|none|∞)?\s*$/i)) {
        return undefined;
    }
    return Number(value);
}
function isValidLimit(value) {
    if (value.match(/^\s*(undef|([+-]?inf)|none|∞)?\s*$/i)) {
        return true;
    }
    try {
        return isFinite(Number(value));
    } catch (e) {
    }
    return false;
}

function lowerBoundChanged() {
    if (!isValidLimit(lowerBoundInput.value)) {
        lowerBoundInput.classList.add('invalid');
        conditionTarget.lower = undefined;

        if (isValidLimit(upperBoundInput.value)) {
            upperBoundInput.classList.remove('invalid');
        }
        return;
    }

    conditionTarget.lower = toLimitValue(lowerBoundInput.value);
    if (!isValidRange()) {
        lowerBoundInput.classList.add('invalid');
        upperBoundInput.classList.add('invalid');
        return;
    }

    lowerBoundInput.classList.remove('invalid');
    if (isValidLimit(upperBoundInput.value)) {
        upperBoundInput.classList.remove('invalid');
    }
}
$(lowerBoundInput).change(lowerBoundChanged);
$(lowerBoundInput).on('input', lowerBoundChanged);

function upperBoundChanged() {
    if (!isValidLimit(upperBoundInput.value)) {
        upperBoundInput.classList.add('invalid');
        conditionTarget.upper = undefined;

        if (isValidLimit(lowerBoundInput.value)) {
            lowerBoundInput.classList.remove('invalid');
        }
        return;
    }

    conditionTarget.upper = toLimitValue(upperBoundInput.value);
    if (!isValidRange()) {
        lowerBoundInput.classList.add('invalid');
        upperBoundInput.classList.add('invalid');
        return;
    }

    upperBoundInput.classList.remove('invalid');
    if (isValidLimit(lowerBoundInput.value)) {
        lowerBoundInput.classList.remove('invalid');
    }
}
$(upperBoundInput).change(upperBoundChanged);
$(upperBoundInput).on('input', upperBoundChanged);
