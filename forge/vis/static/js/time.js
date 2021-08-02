let TimeSelect = {};
let TimeParse = {};
(function() {
    function dayOfWeek(year, month, day) {
        let date = new Date();
        if (year) { date.setUTCFullYear(year); }
        if (month) { date.setUTCMonth(month - 1); }
        else { date.setUTCMonth(0); }
        if (day) { date.setUTCDate(day); }
        else { date.setUTCDate(1); }
        date.setUTCMilliseconds(0);
        date.setUTCSeconds(0);
        date.setUTCMinutes(0);
        date.setUTCHours(0);
        return date.getUTCDay();
    }

    function julianDay(year, month, day) {
        if (!month) { month = 1; }
        if (!day) { day = 1; }
        if (month > 2) {
            month -= 3;
        } else {
            year--;
            month += 9;
        }
        const c = year / 100;
        const ya = year - 100 * c;
        return ((146097 * c) / 4 + (1461 * ya) / 4 + (153 * month + 2) / 5 + day + 1721119);
    }

    function startOfYear(year) {
        let date = new Date();
        date.setUTCFullYear(year);
        date.setUTCMilliseconds(0);
        date.setUTCSeconds(0);
        date.setUTCMinutes(0);
        date.setUTCHours(0);
        date.setUTCDate(1);
        date.setUTCMonth(0);
        return date.getTime();
    }

    function selectedYear(reference) {
        if (reference) {
            return (new Date(Math.round(reference))).getUTCFullYear();
        } else {
            return (new Date.now()).getUTCFullYear();
        }
    }

    const weekOffset = [0, -1, -2, -3, 3, 2, 1];
    function weekStart(year, week) {
        const dow = (dayOfWeek(year) + 6) % 7;
        const baseJD = julianDay(year);
        let jd = baseJD + weekOffset[dow] + (week - 1) * 7;
        if (jd - baseJD < 0) year--;
        let count = 0;
        while ((jd - julianDay(year + (++count))) > 0) { }
        year += count - 1;
        return startOfYear(year) + (jd - julianDay(year)) * 86400.0 * 1000.0;
    }

    const quarterStartDOY = [1, 91, 182, 274];
    function quarterStart(year, quarter) {
        return startOfYear(year) + (quarterStartDOY[quarter-1] - 1) * 86400.0 * 1000.0;
    }
    function quarterEnd(year, quarter) {
        if (quarter === 4) {
            return startOfYear(year+1);
        }
        return startOfYear(year) + (quarterStartDOY[quarter] - 1) * 86400.0 * 1000.0;
    }

    TimeParse.parseTime = function(input, reference, direction) {
        if (input === undefined || input === null) {
            return undefined;
        }
        if (input.match(/^\s*(undef|inf|none|∞)/i)) {
            return null;
        }
        if (input.match(/^\s*now\s*$/i)) {
            return Date.now();
        }
        if (input.match(/^\s*\d{8,}\s*$/)) {
            const ms = parseInt(input.trim());
            if (isFinite(ms)) {
                return ms;
            }
        }
        try {
            if (input.match(/^\s*(w|week)\s*$/i)) {
                return reference + 7 * 86400 * 1000 * direction;
            }
        } catch (e) {
        }

        const weekMatch = input.match(/^\s*(\d{4})?w(\d{1,2})\s*$/i);
        if (weekMatch) {
            try {
                let year = weekMatch[1];
                if (!year) {
                    year = selectedYear(reference);
                } else {
                    year = parseInt(year);
                }
                const week = parseInt(weekMatch[2]);
                if (isFinite(year) && year >= 1970 && year < 2999 &&
                        isFinite(week) && week >= 1 && week <= 53) {
                    const start = weekStart(year, week);
                    if (direction && direction > 0) {
                        return start + 7 * 86400 * 1000;
                    } else {
                        return start;
                    }
                }
            } catch (e) {
            }
        }

        const quarterMatch = input.match(/^\s*(\d{4})?Q([1234])\s*$/i);
        if (quarterMatch) {
            try {
                let year = quarterMatch[1];
                if (!year) {
                    year = selectedYear(reference);
                } else {
                    year = parseInt(year);
                }
                if (isFinite(year) && year >= 1970 && year < 2999) {
                    const quarter = parseInt(quarterMatch[2]);
                    if (direction && direction > 0) {
                        return quarterEnd(year, quarter);
                    } else {
                        return quarterStart(year, quarter);
                    }
                }
            } catch (e) {
            }
        }

        const fractionalYear = input.match(/^\s*(\d{4})\.(\d+)\s*$/i);
        if (fractionalYear) {
            try {
                const year = parseInt(fractionalYear[1]);
                const fraction = parseFloat('0.' + fractionalYear[2]);
                if (isFinite(year) && year >= 1970 && year < 2999 &&
                        isFinite(fraction) && fraction >= 0.0 && fraction <= 1.0) {
                    const start = startOfYear(year);
                    const end = startOfYear(year + 1);
                    return start + (end - start) * fraction;
                }
            } catch (e) {
            }
        }

        const parts = input.trim().split(/\s+|[:TZ-]/i);
        if (parts.length <= 0) {
            return undefined;
        }
        if (parts.length === 1) {
            const first = parts[0];
            try {
                if (first.endsWith("s")) {
                    const offset = parseInt(first.slice(0, -1));
                    if (isFinite(offset) && isFinite(reference) && isFinite(direction)) {
                        return reference + offset * 1000 * direction;
                    }
                } else if (first.endsWith("m")) {
                    const offset = parseInt(first.slice(0, -1));
                    if (isFinite(offset) && isFinite(reference) && isFinite(direction)) {
                        return reference + offset * 60 * 1000 * direction;
                    }
                } else if (first.endsWith("h")) {
                    const offset = parseInt(first.slice(0, -1));
                    if (isFinite(offset) && isFinite(reference) && isFinite(direction)) {
                        return reference + offset * 3600 * 1000 * direction;
                    }
                } else if (first.endsWith("d")) {
                    const offset = parseInt(first.slice(0, -1));
                    if (isFinite(offset) && isFinite(reference) && isFinite(direction)) {
                        return reference + offset * 86400 * 1000 * direction;
                    }
                } else if (first.endsWith("w")) {
                    const offset = parseInt(first.slice(0, -1));
                    if (isFinite(offset) && isFinite(reference) && isFinite(direction)) {
                        return reference + offset * 7 * 86400 * 1000 * direction;
                    }
                }
            } catch (e) {
            }
            try {
                const doy = parseFloat(first);
                if (isFinite(doy) && isFinite(reference) && doy >= 1.0 && doy <= 365.0) {
                    let date = new Date(Math.round(reference));
                    date.setUTCMilliseconds(0);
                    date.setUTCSeconds(0);
                    date.setUTCMinutes(0);
                    date.setUTCHours(0);
                    date.setUTCDate(1);
                    date.setUTCMonth(0);
                    return date.getTime() + Math.round((doy - 1) * 1440.0) * 60.0 * 1000.0;
                }
            } catch (e) {
            }
            return undefined;
        } else if (parts.length === 2) {
            const first = parts[0];
            const second = parts[1];
            try {
                const year = parseInt(first);
                const doy = parseFloat(second);
                if (isFinite(year) && year >= 1970.0 && year < 2999.0 &&
                        isFinite(doy) && doy >= 1.0 && doy <= 366.0) {
                    return startOfYear(year) + Math.round((doy - 1) * 1440.0) * 60.0 * 1000.0;
                }
            } catch (e) {
            }
            return undefined;
        }

        if (parts.length < 3) {
            return undefined;
        }

        let date = new Date();
        date.setUTCMilliseconds(0);
        date.setUTCSeconds(0);
        date.setUTCMinutes(0);
        date.setUTCHours(0);

        try {
            const year = parseInt(parts[0]);
            if (!isFinite(year) || year < 1970 || year > 2999) {
                return undefined;
            }
            date.setUTCFullYear(year);

            const month = parseInt(parts[1]);
            if (!isFinite(month) || month < 1 || month > 12) {
                return undefined;
            }
            date.setUTCMonth(month-1);

            const day = parseInt(parts[2]);
            if (!isFinite(day) || day < 1 || day > 31) {
                return undefined;
            }
            date.setUTCDate(day);

            if (parts.length > 3) {
                const hours = parseInt(parts[3]);
                if (!isFinite(hours) || hours < 0 || hours > 23) {
                    return undefined;
                }
                date.setUTCHours(hours);
            }
            if (parts.length > 4) {
                const minutes = parseInt(parts[4]);
                if (!isFinite(minutes) || minutes < 0 || minutes > 59) {
                    return undefined;
                }
                date.setUTCMinutes(minutes);
            }
            if (parts.length > 5) {
                const seconds = parseInt(parts[5]);
                if (!isFinite(seconds) || seconds < 0 || seconds > 60) {
                    return undefined;
                }
                date.setUTCSeconds(seconds);
            }
        } catch (e) {
            return undefined;
        }

        return date.getTime();
    }
    TimeParse.getImpliedOffset = function(input, time_ms) {
        if (input === undefined || input === null) {
            return undefined;
        }

        const weekMatch = input.match(/^\s*(\d{4})?w(\d{1,2})\s*$/i);
        if (weekMatch) {
            try {
                let year = weekMatch[1];
                if (!year) {
                    year = selectedYear(time_ms);
                } else {
                    year = parseInt(year);
                }
                const week = parseInt(weekMatch[2]);
                if (isFinite(year) && year >= 1970 && year < 2999 &&
                        isFinite(week) && week >= 1 && week <= 53) {
                    return '1w';
                }
            } catch (e) {
            }
        }

        const quarterMatch = input.match(/^\s*(\d{4})?Q([1234])\s*$/i);
        if (quarterMatch) {
            try {
                let year = quarterMatch[1];
                if (!year) {
                    year = selectedYear(time_ms);
                } else {
                    year = parseInt(year);
                }
                if (isFinite(year) && year >= 1970 && year < 2999) {
                    const quarter = parseInt(quarterMatch[2]);
                    return year.toString().padStart(4, '0') + 'Q' + quarter.toString();
                }
            } catch (e) {
            }
        }

        const doyMatch = input.match(/^\s*(?:(\d{4})(?:\s+|:))?(\d{1,3})\s*$/i);
        if (doyMatch) {
            try {
                let year = parseInt(doyMatch[1]);
                if (!year) {
                    year = selectedYear(time_ms);
                } else {
                    year = parseInt(year);
                }
                const doy = parseFloat(doyMatch[2]);
                if (isFinite(year) && year >= 1970 && year < 2999 &&
                        isFinite(doy) && doy >= 1.0 && doy <= 366.0) {
                    return "1d";
                }
            } catch (e) {
            }
        }

        const dateMatch = input.match(/^\s*(\d{4})(?:\s+|-)(\d{2})(?:\s+|-)(\d{2})\s*$/i);
        if (dateMatch) {
            try {
                const year = parseInt(dateMatch[1]);
                const month = parseInt(dateMatch[2]);
                const day = parseInt(dateMatch[3]);
                if (isFinite(year) && year >= 1970 && year < 2999 &&
                        isFinite(month) && month >= 1 && month <= 12 &&
                        isFinite(day) && day >= 1 && day <= 31) {
                    return "1d";
                }
            } catch (e) {
            }
        }

        return undefined;
    }
    TimeParse.isOffset = function(input) {
        if (input.match(/^\s*\d+[smhdw]\s*$/i)) {
            return true;
        }
        return false;
    }

    TimeParse.toDisplayTime = function(epoch_ms, padding, separator) {
        if (!epoch_ms || !isFinite(epoch_ms)) {
            return "∞";
        }
        let date = new Date(Math.floor(epoch_ms));

        if (localStorage.getItem('forge-settings-time-format') === 'doy') {
            let doyDigits = 5;

            date.setUTCMilliseconds(0);
            date.setUTCSeconds(0);
            date.setUTCMinutes(0);
            if (date.getTime() === Math.floor(epoch_ms)) {
                doyDigits = 2;
            }
            date.setUTCHours(0);
            if (date.getTime() === Math.floor(epoch_ms)) {
                doyDigits = 0;
            }
            date.setUTCDate(1);
            date.setUTCMonth(0);
            const yearStart = date.getTime();
            const doy = (epoch_ms - yearStart) / (86400.0 * 1000) + 1;
            return (
                date.getUTCFullYear().toString().padStart(4, '0') +
                (separator === undefined ? ':' : separator) +
                (padding === undefined ? doy.toFixed(doyDigits) : doy.toFixed(5).padStart(9, padding))
            )
        }

        return (
            date.getUTCFullYear().toString().padStart(4, '0') + '-' +
            (date.getUTCMonth()+1).toString().padStart(2, '0') + '-' +
            date.getUTCDate().toString().padStart(2, '0') +
            (separator === undefined ? 'T' : separator) +
            date.getUTCHours().toString().padStart(2, '0') + ':' +
            date.getUTCMinutes().toString().padStart(2, '0') + ':' +
            date.getUTCSeconds().toString().padStart(2, '0') + 'Z'
        );
    }

    const runWhenChanged = new Map();
    TimeSelect.onChanged = function(key, cb) {
        runWhenChanged.set(key, cb);
    }
    TimeSelect.change = function(start_ms, end_ms) {
        TimeSelect.start_ms = start_ms;
        TimeSelect.end_ms = end_ms;
        TimeSelect.zoom_start_ms = undefined;
        TimeSelect.zoom_end_ms = undefined;
    
        localStorage.setItem('forge-last-start', TimeSelect.start_ms.toString());
        localStorage.setItem('forge-last-end', TimeSelect.end_ms.toString());
    
        runWhenChanged.forEach((cb) => {
            cb();
        });
    }
    
    
    TimeSelect.setDefaultTimeRange = function(showDays) {
        let currentUTCDay = new Date();
        currentUTCDay.setUTCMilliseconds(0);
        currentUTCDay.setUTCSeconds(0);
        currentUTCDay.setUTCMinutes(0);
        currentUTCDay.setUTCHours(0);
    
        if (showDays === undefined) {
            showDays = 7;
        }
    
        const end_ms = currentUTCDay.getTime();
        const start_ms = end_ms - showDays * 86400 * 1000;
        TimeSelect.change(start_ms, end_ms);
    };

    TimeSelect.fetchLatestPassed = undefined;

    const queryParameters = new URLSearchParams(window.location.search);
    TimeSelect.start_ms = TimeParse.parseTime(queryParameters.get('start'));
    TimeSelect.end_ms = TimeParse.parseTime(queryParameters.get('end'));
    if (isFinite(TimeSelect.start_ms) && !isFinite(TimeSelect.end_ms)) {
        TimeSelect.end_ms = TimeParse.parseTime(
            TimeParse.getImpliedOffset(queryParameters.get('start'), TimeSelect.start_ms),
            TimeSelect.start_ms, 1);
    } else if (!isFinite(TimeSelect.start_ms) && isFinite(TimeSelect.end_ms)) {
        TimeSelect.start_ms = TimeParse.parseTime(
            TimeParse.getImpliedOffset(queryParameters.get('end'), TimeSelect.end_ms),
            TimeSelect.end_ms, -1);
    }

    if (!isFinite(TimeSelect.start_ms) || !isFinite(TimeSelect.end_ms))  {
        TimeSelect.start_ms = parseInt(localStorage.getItem('forge-last-start'));
        TimeSelect.end_ms = parseInt(localStorage.getItem('forge-last-end'));
    }
    if (!isFinite(TimeSelect.start_ms) || !isFinite(TimeSelect.end_ms)) {
        TimeSelect.setDefaultTimeRange();
    }
    
    let original_start_ms = TimeSelect.start_ms;
    let original_end_ms = TimeSelect.end_ms;
    TimeSelect.resetTimeRange = function() {
        TimeSelect.change(original_start_ms, original_end_ms);
    }

    let runOnHightlight = function(start_ms, end_ms) {};
    const activeHighlights = new Map();
    function getActiveHighlight() {
        let active = undefined;
        activeHighlights.forEach((highlight) => {
            if (!active || highlight.priority > active.priority) {
                active = highlight;
            }
        });
        return active;
    }
    TimeSelect.onHighlight = function(cb) {
        runOnHightlight = cb;
        const active = getActiveHighlight();
        if (active) {
            runOnHightlight(active.start_ms, active.end_ms);
        }
    }
    TimeSelect.highlight = function(key, start_ms, end_ms, priority) {
        if (!priority) {
            priority = 0;
        }
        const prior = getActiveHighlight();
        if (start_ms === undefined && end_ms === undefined) {
            activeHighlights.delete(key);
        } else {
            activeHighlights.set(key, {
                priority: priority,
                start_ms: start_ms,
                end_ms: end_ms,
            });
        }
        const active = getActiveHighlight();
        if (active === prior) {
            return;
        }
        if (!active) {
            runOnHightlight(undefined, undefined);
        } else {
            runOnHightlight(active.start_ms, active.end_ms);
        }
    }

    const runOnZoom = new Map();
    TimeSelect.zoom_start_ms = undefined;
    TimeSelect.zoom_end_ms = undefined;
    TimeSelect.onZoom = function(key, cb) {
        runOnZoom.set(key, cb);
    }
    TimeSelect.zoom = function(start_ms, end_ms) {
        TimeSelect.zoom_start_ms = start_ms;
        TimeSelect.zoom_end_ms = end_ms;
        runOnZoom.forEach((cb) => {
            cb();
        });
    }
})();

