function newtonRaphson(yTarget, evaluate, initial, firstStep, completed) {
    let x0 = 0.0;
    if (initial !== undefined) {
        x0 = initial;
    }
    if (typeof x0 === 'function') {
        x0 = initial(yTarget);
    }
    if (!isFinite(x0)) {
        return undefined;
    }

    let y0 = evaluate(x0);
    if (!isFinite(y0)) {
        return undefined;
    }

    let x1 = firstStep;
    if (x1 === undefined) {
        if (x0 === 0.0) {
            x1 = 0.1;
        } else {
            x1 = x0 * 0.9;
        }
    } else if (typeof x1 === 'function') {
        x1 = x1(x0, y0, yTarget);
    }
    if (!isFinite(x1)) {
        return undefined;
    }

    if (!completed) {
        completed = function(dX) {
            return Math.abs(dX) < 1E-6;
        }
    }

    for (let itter = 0; itter < 20; itter++) {
        const y1 = evaluate(x1);
        if (!isFinite(y1)) {
            return undefined;
        }
        if (y1 === y0) {
            return x1;
        }

        const dX = ((x1 - x0) / (y1 - y0)) * (yTarget - y1);
        if (completed(dX, x0, x1, y0, y1, yTarget)) {
            return x1 + dX;
        }
        x0 = x1;
        y0 = y1;
        x1 += dX;
    }
    return undefined;
}