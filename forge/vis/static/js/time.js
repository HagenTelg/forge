let TimeSelect = {};
let TimeParse = {};
(function() {
    function dayOfWeek(year, month, day) {
        const date = new Date(Date.UTC(
            year || (new Date()).getUTCFullYear(),
            (month || 1) - 1,
            day || 1
        ));
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
        const c = Math.floor(year / 100);
        const ya = year - 100 * c;
        return Math.floor((146097 * c) / 4) + Math.floor((1461 * ya) / 4) +
            Math.floor((153 * month + 2) / 5) + day + 1721119;
    }

    function startOfYear(year) {
        return (new Date(Date.UTC(year, 0))).getTime();
    }

    function selectedYear(reference) {
        if (reference) {
            return (new Date(Math.round(reference))).getUTCFullYear();
        } else {
            return (new Date()).getUTCFullYear();
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

            const month = parseInt(parts[1]);
            if (!isFinite(month) || month < 1 || month > 12) {
                return undefined;
            }

            const day = parseInt(parts[2]);
            if (!isFinite(day) || day < 1 || day > 31) {
                return undefined;
            }

            date = new Date(Date.UTC(year, month-1, day));

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
    TimeParse.parseInterval = function(input, disableBareDay) {
        if (input === undefined || input === null) {
            return undefined;
        }
        if (input.match(/^\s*(undef|inf|∞)/i)) {
            return null;
        }
        if (input.match(/^\s*none\s*$/i)) {
            return 0;
        }

        let match;

        match = input.match(/^\s*(\d+(?:\.\d*)?)\s*S(?:econds?)?\s*$/i)
        if (match) {
            try {
                const interval = parseFloat(match[1]);
                if (isFinite(interval) && interval > 0.0) {
                    return interval * 1000;
                }
            } catch (e) {
            }
        }

        match = input.match(/^\s*(\d+(?:\.\d*)?)\s*M(?:inutes?)?\s*$/i)
        if (match) {
            try {
                const interval = parseFloat(match[1]);
                if (isFinite(interval) && interval > 0.0) {
                    return Math.round(interval * 60) * 1000;
                }
            } catch (e) {
            }
        }

        match = input.match(/^\s*(\d+(?:\.\d*)?)\s*H(?:ours?)?\s*$/i)
        if (match) {
            try {
                const interval = parseFloat(match[1]);
                if (isFinite(interval) && interval > 0.0) {
                    return Math.round(interval * 60) * 60 * 1000;
                }
            } catch (e) {
            }
        }

        match = input.match(/^\s*(\d+(?:\.\d*)?)\s*D(?:ays?)?\s*$/i)
        if (match) {
            try {
                const interval = parseFloat(match[1]);
                if (isFinite(interval) && interval > 0.0) {
                    const parts = match[1].split('.');
                    if (parts.length === 2 && parts[1].length === 2) {
                        return Math.round(interval * 24) * 3600 * 1000;
                    } else {
                        return Math.round(interval * 86400) * 1000;
                    }
                }
            } catch (e) {
            }
        }

        match = input.match(/^\s*(\d+(?:\.\d*)?)\s*W(?:eeks)?\s*$/i)
        if (match) {
            try {
                const interval = parseFloat(match[1]);
                if (isFinite(interval) && interval > 0.0) {
                    return Math.round(interval * 7 * 86400) * 1000;
                }
            } catch (e) {
            }
        }

        if (disableBareDay) {
            return undefined;
        }

        try {
            const interval = parseFloat(input);
            if (isFinite(interval) && interval > 0.0) {
                const parts = input.split('.');
                if (parts.length === 2 && parts[1].length === 2) {
                    return Math.round(interval * 24) * 3600 * 1000;
                } else {
                    return Math.round(interval * 86400) * 1000;
                }
            }
        } catch (e) {
        }

        return undefined;
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

        const doyMatch = input.match(/^\s*(\d{4})(?:\s+|:)(\d{1,3})\s*$/i);
        if (doyMatch) {
            try {
                let year = doyMatch[1];
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
    TimeParse.toDisplayInterval = function(ms) {
        if (!ms || !isFinite(ms)) {
            return "∞";
        }

        if ((ms % (7 * 86400 * 1000)) === 0) {
            const count = (ms / (7 * 86400 * 1000));
            if (count === 1) {
                return "1 Week"
            }
            return count.toFixed(0) + " Weeks";
        }

        const days = ms / (86400 * 1000);
        if ((ms % (86400 * 1000)) === 0) {
            if (days === 1) {
                return "1 Day"
            }
            return days.toFixed(0) + " Days";
        }

        if (days > 3.0) {
            if ((ms % (3600 * 1000)) === 0) {
                return days.toFixed(2) + " Days";
            }
            return days.toFixed(5) + " Days";
        }

        if ((ms % (3600 * 1000)) === 0) {
            const count = Math.round(ms / (3600 * 1000));
            if (count === 1) {
                return "1 Hour"
            }
            return count.toFixed(0) + " Hours";
        }

        const count = Math.round(ms / (60 * 1000));
        if (count === 1) {
            return "1 Minute"
        }
        return count.toFixed(0) + " Minutes";
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
    TimeSelect.changeInterval = function(interval_ms) {
        TimeSelect.interval_ms = interval_ms;

        TimeSelect.setIntervalBounds();
        TimeSelect.zoom_start_ms = undefined;
        TimeSelect.zoom_end_ms = undefined;

        localStorage.setItem('forge-last-interval', TimeSelect.interval_ms.toString());

        runWhenChanged.forEach((cb) => {
            cb();
        });
    }

    function applyDefaultTimeRange(showDays) {
        let currentUTCDay = new Date();
        currentUTCDay.setUTCMilliseconds(0);
        currentUTCDay.setUTCSeconds(0);
        currentUTCDay.setUTCMinutes(0);
        currentUTCDay.setUTCHours(0);

        if (showDays === undefined) {
            showDays = 7;
        }

        TimeSelect.end_ms = currentUTCDay.getTime();
        TimeSelect.start_ms = TimeSelect.end_ms - showDays * 86400 * 1000;
    }
    TimeSelect.setDefaultTimeRange = function(showDays) {
        applyDefaultTimeRange(showDays)
        TimeSelect.change(TimeSelect.start_ms, TimeSelect.end_ms);
    };

    function applyDefaultInterval(showDays) {
        if (showDays === undefined) {
            showDays = 1;
        }

        TimeSelect.interval_ms = showDays * 86400 * 1000;
    }
    TimeSelect.setDefaultInterval = function() {
        applyDefaultInterval();
        TimeSelect.changeInterval(TimeSelect.interval_ms);
    }

    TimeSelect.fetchLatestPassed = undefined;

    const queryParameters = new URLSearchParams(window.location.search);
    TimeSelect.start_ms = TimeParse.parseTime(queryParameters.get('start'));
    TimeSelect.end_ms = TimeParse.parseTime(queryParameters.get('end'));
    if (isFinite(TimeSelect.start_ms) && !isFinite(TimeSelect.end_ms)) {
        TimeSelect.end_ms = TimeParse.parseTime(queryParameters.get('end') ||
            TimeParse.getImpliedOffset(queryParameters.get('start'), TimeSelect.start_ms),
            TimeSelect.start_ms, 1);
    } else if (!isFinite(TimeSelect.start_ms) && isFinite(TimeSelect.end_ms)) {
        TimeSelect.start_ms = TimeParse.parseTime(queryParameters.get('start') ||
            TimeParse.getImpliedOffset(queryParameters.get('end'), TimeSelect.end_ms),
            TimeSelect.end_ms, -1);
    }

    if (!isFinite(TimeSelect.start_ms) || !isFinite(TimeSelect.end_ms))  {
        TimeSelect.start_ms = parseInt(localStorage.getItem('forge-last-start'));
        TimeSelect.end_ms = parseInt(localStorage.getItem('forge-last-end'));
    }
    if (!isFinite(TimeSelect.start_ms) || !isFinite(TimeSelect.end_ms)) {
        applyDefaultTimeRange();
    }

    TimeSelect.interval_ms = TimeParse.parseInterval(queryParameters.get('interval'));
    if (!isFinite(TimeSelect.interval_ms) || TimeSelect.interval_ms <= 0.0)  {
        TimeSelect.interval_ms = parseInt(localStorage.getItem('forge-last-interval'));
    }
    if (!isFinite(TimeSelect.interval_ms) || TimeSelect.interval_ms <= 0.0)  {
        applyDefaultInterval();
    }
    
    let original_start_ms = TimeSelect.start_ms;
    let original_end_ms = TimeSelect.end_ms;
    TimeSelect.resetTimeRange = function() {
        TimeSelect.change(original_start_ms, original_end_ms);
    }
    TimeSelect.updateResetRange = function() {
        original_start_ms = TimeSelect.start_ms;
        original_end_ms = TimeSelect.end_ms;
    }

    let original_interval_ms = TimeSelect.interval_ms;
    TimeSelect.resetInterval = function() {
        TimeSelect.changeInterval(original_interval_ms);
    }

    TimeSelect.setIntervalBounds = function() {
        const end_ms = (new Date()).getTime();
        TimeSelect.start_ms = end_ms - TimeSelect.interval_ms;
        TimeSelect.end_ms = end_ms;

        original_start_ms = TimeSelect.start_ms;
        original_end_ms = TimeSelect.end_ms;
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
    TimeSelect.isZoomed = function() {
        return !!(TimeSelect.zoom_start_ms || TimeSelect.zoom_start_ms);
    }
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
    TimeSelect.applyZoom = function(start_ms, end_ms) {}
    TimeSelect.resetZoomConnections = function() {
        TimeSelect.applyZoom = function(start_ms, end_ms) {};
        TimeSelect.zoom_start_ms = undefined;
        TimeSelect.zoom_end_ms = undefined;
    }

    TimeSelect.onIntervalHeartbeat = function() {}
    TimeSelect.resetIntervalHeartbeat = function() {
        TimeSelect.onIntervalHeartbeat = function() {}
    }
    function runIntervalHeartbeat() {
        TimeSelect.onIntervalHeartbeat();
    }
    setInterval(runIntervalHeartbeat, 5 * 60 * 1000);
})();

