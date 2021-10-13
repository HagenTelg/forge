Calibration.target = [];


function addCoefficient(index) {
    if (!isFinite(index)) {
        index = Calibration.target.length;
    }
    while (index >= Calibration.target.length) {
        Calibration.target.push(0);
    }

    if (index > 0) {
        const separator = document.createElement('span');
        separator.classList.add('details-calibration-separator');
        separator.textContent = '+';
        Calibration.list.appendChild(separator);
    }

    const span = document.createElement('span');
    span.classList.add('details-calibration-coefficient');
    Calibration.list.appendChild(span);

    const inputId = Calibration.valueLabel + index.toString();
    const input = document.createElement('input');
    input.classList.add('details-calibration-coefficient-value');
    input.type = 'text';
    input.id = inputId;
    input.value = Calibration.target[index].toString();
    span.appendChild(input);

    if (index > 0) {
        const label = document.createElement('label');
        label.htmlFor = inputId;
        span.appendChild(label);

        label.textContent = 'x';
        if (index > 1) {
            const sup = document.createElement('sup');
            sup.textContent = index.toString();
            label.appendChild(sup);
        }
    }

    function isValidCoefficient(value) {
        try {
            return isFinite(Number(value));
        } catch (e) {
        }
        return false;
    }
    function valueChanged() {
        if (isValidCoefficient(input.value)) {
            input.classList.remove('invalid');
            Calibration.target[index] = Number(input.value);
        } else {
            input.classList.add('invalid');
            Calibration.target[index] = 0;
        }
    }
    $(input).change(valueChanged);
    $(input).on('input', valueChanged);

    return input;
}

$(Calibration.addCoefficient).click(function(event) {
    event.preventDefault();
    addCoefficient();
});


Calibration.configure = function(directive, field) {
    if (!field) {
        field = 'calibration';
    }
    if (!directive[field]) {
        directive[field] = [0, 1];
    }
    if (directive[field].length <= 0) {
        directive[field] = [0];
    }

    Calibration.target = directive[field];

    while (Calibration.list.firstChild) {
        Calibration.list.removeChild(Calibration.list.firstChild);
    }

    Calibration.target.forEach((coefficient, index) => {
        addCoefficient(index);
    });
};