let CONTROL_CONTEXT = undefined;

if (!sessionStorage.getItem('forge-acquisition-user-name')) {
    sessionStorage.setItem('forge-acquisition-user-name', "{{ request.user.initials|e }}")
}

localStorage.setItem('forge-last-station', '{{ station }}');
localStorage.setItem('forge-last-mode', '{{ mode.mode_name }}');

$(document).ready(function() {
    CONTROL_CONTEXT = new AcquisitionSocket.DispatchContext();

    const summaryContent = document.getElementById('summary_content');
    const displayContent = document.getElementById('display_content');
    const communicationIndicators = document.getElementById('communication_indicators');

    function isModalActive() {
        const modal = document.getElementById('modal-container');
        return modal.style.display === 'block';
    }

    let uidCounter = 0;
    function allocateUID() {
        uidCounter += 1;
        return "acq" + uidCounter.toString();
    }

    function applyPadding(formatted, rightPad, totalLength) {
        if (rightPad > 0) {
            formatted = formatted + "\u00A0".repeat(rightPad);
        }
        if (formatted.length < totalLength) {
            const pad = totalLength - formatted.length;
            formatted = "\u00A0".repeat(pad) + formatted;
        }
        return formatted;
    }

    class ListContext extends AcquisitionSocket.DispatchContext {
        constructor(source) {
            super();
            this.source = source;
            this.root = undefined;
            this._activeTimeouts = new Set();

            if (source) {
                this.addInstrumentRemove(() => {
                    this.detach();
                }, source);
            }
        }

        attachToList(listTarget, beforeIndex) {
            this.root = document.createElement('li');
            this.root.context = this;

            if (typeof beforeIndex === 'undefined' || beforeIndex >= listTarget.children.length) {
                listTarget.appendChild(this.root);
            } else {
                listTarget.insertBefore(this.root, listTarget.children[beforeIndex]);
            }
        }

        detach() {
            super.detach();

            for (const id of this._activeTimeouts) {
                clearTimeout(id);
            }
            this._activeTimeouts.clear();

            if (!this.root) {
                return;
            }
            this.root.parentNode.removeChild(this.root);
            this.root = undefined;
        }

        createNodeFormatter(target) {
            const decimals = target.getAttribute('decimals') || 1;
            const rightPad = target.getAttribute('rightpad') || 0;
            const totalLength = target.getAttribute('totallength') || 0;
            const exponential = target.hasAttribute('exponential');
            const mvc = target.getAttribute('mvc') || "";

            return (value) => {
                if (value === null || !isFinite(value)) {
                    target.textContent = mvc;
                    return;
                }

                let formatted;
                if (exponential) {
                    formatted = (value * 1.0).toExponential(decimals);
                } else {
                    formatted = (value * 1.0).toFixed(decimals);
                }

                target.textContent = applyPadding(formatted, rightPad, totalLength);
            }
        }

        attachFormatting() {
            if (!this.root) {
                return;
            }

            const context = this;

            $('.value-formatted', context.root).each(function() {
                const field = this.getAttribute('field') || undefined;
                if (!field) {
                    return;
                }
                context.addSourceTarget(context.createNodeFormatter(this), context.source, field);
            });

            $('.value-raw', context.root).each(function() {
                const target = this;
                const field = this.getAttribute('field') || undefined;
                if (!field) {
                    return;
                }
                const rightPad = this.getAttribute('rightpad') || 0;
                const totalLength = this.getAttribute('totallength') || 0;
                const mvc = this.getAttribute('mvc') || "";
                context.addSourceTarget((value) => {
                    if (value === undefined || value === null) {
                        target.textContent = mvc;
                        return;
                    }

                    target.textContent = applyPadding(value.toString(), rightPad, totalLength);
                }, context.source, field);
            });

            $('.value-hex', context.root).each(function() {
                const target = this;
                const field = this.getAttribute('field') || undefined;
                if (!field) {
                    return;
                }
                const rightPad = this.getAttribute('rightpad') || 0;
                const totalLength = this.getAttribute('totallength') || 0;
                const digits = this.getAttribute('digits') || 0;
                const mvc = this.getAttribute('mvc') || "";
                const prefix = this.getAttribute('prefix') || "";
                const lowerCase = this.getAttribute('lowercase') || false;
                context.addSourceTarget((value) => {
                    if (value === null || !isFinite(value)) {
                        target.textContent = mvc;
                        return;
                    }

                    value = Math.floor(value * 1);
                    let formatted = value.toString(16);
                    if (lowerCase) {
                        formatted = formatted.toLowerCase();
                    } else {
                        formatted = formatted.toUpperCase();
                    }
                    if (formatted.length < digits) {
                        const pad = digits - formatted.length;
                        formatted = "0".repeat(pad) + formatted;
                    }

                    target.textContent = applyPadding(prefix + formatted, rightPad, totalLength);
                }, context.source, field);
            });

            $('.value-exists', context.root).each(function() {
                const target = this;
                const field = this.getAttribute('field') || undefined;
                if (!field) {
                    return;
                }

                target.classList.add('exists-false');
                target.classList.remove('exists-true');

                for (let f of field.split(/(\s+)/)) {
                    f = f.trim();
                    if (f.length === 0) {
                        continue;
                    }
                    context.addSourceTarget((value) => {
                        if (value === null || value === undefined) {
                            return;
                        }
                        if (typeof value === 'number') {
                            if (!isFinite(value)) {
                                return;
                            }
                        }
                        target.classList.add('exists-true');
                        target.classList.remove('exists-false');
                    }, context.source, f);
                }
            });
        }

        attachState() {
            if (!this.root) {
                return;
            }

            const context = this;
            $('.communication-state', context.root).each(function() {
                const target = this;
                context.addInstrumentState((source, state) => {
                    if (state.communicating) {
                        target.classList.add('communication-true');
                        target.classList.remove('communication-false');
                    } else {
                        target.classList.add('communication-false');
                        target.classList.remove('communication-true');
                    }
                }, context.source);
            });
            $('.bypass-state', context.root).each(function() {
                const target = this;
                context.addInstrumentState((source, state) => {
                    if (state.bypassed) {
                        target.classList.add('bypass-true');
                        target.classList.remove('bypass-false');
                    } else {
                        target.classList.add('bypass-false');
                        target.classList.remove('bypass-true');
                    }
                }, context.source);
            });
            $('.warning-state', context.root).each(function() {
                const target = this;
                context.addInstrumentState((source, state) => {
                    if (state.warning) {
                        target.classList.add('warning-true');
                        target.classList.remove('warning-false');
                    } else {
                        target.classList.add('warning-false');
                        target.classList.remove('warning-true');
                    }
                }, context.source);
            });
            $('.notification-state', context.root).each(function() {
                const target = this;
                const checkPresent = this.getAttribute('notification').split(',') || undefined;
                if (!checkPresent || checkPresent.length === 0) {
                    return;
                }
                function hasNotification(notifications) {
                    if (!notifications) {
                        return false;
                    }
                    for (const n of checkPresent) {
                        if (notifications.includes(n)) {
                            return true;
                        }
                    }
                    return false;
                }
                context.addInstrumentState((source, state) => {
                    if (hasNotification(state.notifications)) {
                        target.classList.add('notification-true');
                        target.classList.remove('notification-false');
                    } else {
                        target.classList.add('notification-false');
                        target.classList.remove('notification-true');
                    }
                }, context.source);
            });
        }

        attachSource() {
            if (!this.root) {
                return;
            }
            const context = this;
            $('.format-source', context.root).each(function() {
                const target = this;
                const instrument = target.getAttribute('instrument');

                context.addInstrumentPresent((source, info) => {
                    const serial_number = info.serial_number;
                    const display_id = info.display_id;
                    const manufacturer = info.manufacturer;
                    const model = info.model;

                    if (instrument) {
                        let display = instrument;
                        if (serial_number) {
                            display = display + " #" + serial_number;
                        }
                        if (display_id) {
                            display = display + " (" + display_id + ")";
                        }
                        target.textContent = display;
                    } else if (display_id) {
                        let display = display_id;
                        if (serial_number) {
                            display = display + " #" + serial_number;
                        }
                        target.textContent = display;
                    } else if (manufacturer && model) {
                        let display = manufacturer + " " + model;
                        if (serial_number) {
                            display = display + " #" + serial_number;
                        }
                        target.textContent = display;
                    }
                }, context.source);
            });
        }

        hide() {
            if (!this.root) {
                return;
            }
            this.root.style.display = 'none';
        }

        show() {
            if (!this.root) {
                return;
            }
            this.root.style.display = '';
        }

        tickIn(call, delay_ms) {
            if (!delay_ms) {
                delay_ms = 0;
            }

            let id = setTimeout(() => {
                if (!this.root) {
                    return;
                }
                this._activeTimeouts.delete(id);
                id = undefined;

                call();
            }, delay_ms);
            this._activeTimeouts.add(id);

            return () => {
                if (!id) {
                    return;
                }
                clearTimeout(id);
            }
        }

        tickAt(call, epoch_ms) {
            function delayToTarget() {
                const now = Date.now();
                if (now >= epoch_ms) {
                    return 0;
                }
                const delay = epoch_ms - now;
                if (delay < 100) {
                    return 100;
                }
                return delay;
            }

            let id = undefined;
            const process = () => {
                if (!this.root) {
                    return;
                }
                this._activeTimeouts.delete(id);
                id = undefined;

                const delay = delayToTarget();
                if (delay > 0) {
                    id = setTimeout(process, delay);
                    this._activeTimeouts.add(id);
                    return;
                }
                id = undefined;
                call();
            }

            id = setTimeout(process, delayToTarget());
            this._activeTimeouts.add(id);
            return () => {
                if (!id) {
                    return;
                }
                clearTimeout(id);
            }
        }

        tickOnSecond(call, epoch_ms) {
            function delayToNext() {
                const now = Date.now();
                if (now >= epoch_ms) {
                    return 0;
                }
                let delay = epoch_ms - now;
                if (delay < 100) {
                    return 100;
                }

                delay = delay % 1000;
                delay += 10;
                return delay;
            }

            let id = undefined;
            const process = () => {
                if (!this.root) {
                    return;
                }
                this._activeTimeouts.delete(id);
                id = undefined;

                const delay = delayToNext();
                if (delay > 0) {
                    id =  setTimeout(process, delay);
                    this._activeTimeouts.add(id);
                }
                call();
            }

            id = setTimeout(process, delayToNext());
            this._activeTimeouts.add(id);
            return () => {
                if (!id) {
                    return;
                }
                clearTimeout(id);
            }
        }
    }


    function sortSummary() {
        let summaryEntries = Array.from(summaryContent.childNodes);
        summaryEntries.sort((a, b) => {
            const ca = a.context;
            const cb = b.context;
            if (ca.base.priority < cb.base.priority) {
                return 1;
            } else if (ca.base.priority > cb.base.priority) {
                return -1;
            }
            const ka = a.sortKey;
            const kb = b.sortKey;
            if (ka) {
                if (!kb) {
                    return 1;
                }
                if (ka < kb) {
                    return -1;
                } else if (ka > kb) {
                    return 1;
                }
            } else if (kb) {
                return -1;
            }
            return 0;
        });
        summaryEntries.forEach(row => summaryContent.appendChild(row));
    }

    class SummaryContext extends ListContext {
        constructor(uid, base, source) {
            super(source);
            this.uid = uid;
            this.base = base;

            this.instrument_info = {};
            if (source) {
                this.addInstrumentPresent((source, info) => {
                   this.instrument_info = info;
                }, source);
            }
        }

        get sortKey() {
            const displayID = this.instrument_info.display_id;
            if (displayID) {
                return displayID;
            }
            return this.source;
        }

        attach() {
            this.attachToList(summaryContent);
            this.root.id = this.uid;
            $(this.root).load(this.base.target(this.uid, this.source));
            sortSummary();
        }

        standardMode() {
            this.attachFormatting();
            this.attachState();
            this.attachSource();
        }
    }


    function saveDisplays() {
        const data = [];
        for (let i=0; i<displayContent.children.length; i++) {
            const display = displayContent.children[i];
            const context = display.context;

            if (!context.source && !context.base.restore_key) {
                continue;
            }

            let isCollapsed = undefined;
            $('.display-container.collapsable', display).each(function() {
                isCollapsed = this.classList.contains('collapsed');
            });

            data.push({
                source: context.source,
                type: context.instrument_info.type,
                key: context.base.restore_key,
                collapsed: isCollapsed,
            });
        }

        localStorage.setItem('forge-acquisition-displays', JSON.stringify(data));
    }

    function matchSavedDisplay(saved, context) {
        let result = false;

        if (saved.source) {
            if (saved.source !== context.source) {
                return false;
            }
            result = true;
        }

        if (saved.key) {
            if (saved.key !== context.base.restore_key) {
                return false;
            }
            result = true;
        }

        if (saved.type) {
            if (saved.type !== context.instrument_info.type) {
                return false;
            }
            result = true;
        }

        return result;
    }

    class DisplayContext extends ListContext {
        constructor(uid, base, source) {
            super(source);
            this.uid = uid;
            this.base = base;
            this._restore = undefined;

            this.instrument_info = {};
            if (source) {
                this.addInstrumentPresent((source, info) => {
                   this.instrument_info = info;
                }, source);
            }
        }

        attach() {
            let restoreData = localStorage.getItem('forge-acquisition-displays');
            if (!restoreData) {
                restoreData = [];
            } else {
                try {
                    restoreData = JSON.parse(restoreData);
                } catch (e) {
                    restoreData = [];
                }
            }

            let restoreIndex = 0;
            for (; restoreIndex<restoreData.length; restoreIndex++) {
                if (matchSavedDisplay(restoreData[restoreIndex], this)) {
                    break;
                }
            }

            let insertBefore = undefined;
            if (restoreIndex < restoreData.length) {
                this._restore = restoreData[restoreIndex];

                function searchBackward() {
                    for (let checkIndex=restoreIndex-1; checkIndex>=0; checkIndex--) {
                        const checkRestore = restoreData[checkIndex];

                        for (let i=0; i<displayContent.children.length; i++) {
                            const display = displayContent.children[i];
                            if (matchSavedDisplay(checkRestore, display.context)) {
                                return i+1;
                            }
                        }
                    }

                    return undefined;
                }

                function searchForward() {
                    for (let checkIndex=restoreIndex+1; checkIndex<restoreData.length; checkIndex++) {
                        const checkRestore = restoreData[checkIndex];

                        for (let i=displayContent.children.length-1; i>=0; i--) {
                            const display = displayContent.children[i];
                            if (matchSavedDisplay(checkRestore, display.context)) {
                                return i;
                            }
                        }
                    }

                    return undefined;
                }

                insertBefore = searchBackward();
                if (typeof insertBefore === 'undefined') {
                    insertBefore = searchForward();
                }
            }

            this.attachToList(displayContent, insertBefore);
            this.root.id = this.uid;
            $(this.root).load(this.base.target(this.uid, this.source));
        }

        attachCollapse() {
            if (!this.root) {
                return;
            }
            $('.display-container.collapsable > .header', this.root).click(function(event) {
                event.preventDefault();
                $(this).parent().toggleClass('collapsed');
                saveDisplays();
            });

            if (this._restore && typeof this._restore.collapsed !== 'undefined') {
                if (this._restore.collapsed) {
                    $('.display-container.collapsable', this.root).addClass('collapsed')
                } else {
                    $('.display-container.collapsable', this.root).removeClass('collapsed')
                }
            }
        }

        activateLetter() {
            if (!this.root) {
                return;
            }

            let expanded = true;
            $('.display-container.collapsable.collapsed', this.root).each(function() {
                this.classList.remove('collapsed');
                expanded = false;
            });
            if (!expanded) {
                return;
            }

            Array.from(displayContent.childNodes).forEach((item) => {
                if (item === this.root) {
                    return;
                }
                displayContent.appendChild(item)
            });
        }

        alterPosition(shift) {
            if (!this.root) {
                return;
            }

            let displayEntries = [];
            let hiddenEntries = [];
            for (let i=0; i<displayContent.childNodes.length; i++) {
                let r = displayContent.childNodes[i];
                if (r.style.display === 'none') {
                    hiddenEntries.push(r);
                } else {
                    displayEntries.push(r);
                }
            }

            let targetIndex = -1;
            for (let i=0; i<displayEntries.length; i++) {
                const display = displayEntries[i];
                if (display === this.root) {
                    targetIndex = i;
                    break;
                }
            }
            if (targetIndex === -1) {
                return;
            }

            displayEntries.splice(targetIndex, 1);

            targetIndex += shift;
            if (targetIndex > displayEntries.length) {
                targetIndex = displayEntries.length;
            }
            if (targetIndex < 0) {
                targetIndex = 0;
            }
            displayEntries.splice(targetIndex, 0, this.root);

            hiddenEntries.concat(displayEntries).forEach(display => displayContent.appendChild(display));
        }

        attachMove() {
            if (!this.root) {
                return;
            }

            const context = this;
            $('.move-back', this.root).click(function(event) {
                event.preventDefault();
                context.alterPosition(-1);
                saveDisplays();
            });
            $('.move-forward', this.root).click(function(event) {
                event.preventDefault();
                context.alterPosition(1);
                saveDisplays();
            });
        }

        attachCommandButtons() {
            if (!this.root) {
                return;
            }

            const context = this;

            $('button.simple-command', context.root).click(function(event) {
                event.preventDefault();

                const command = $(this).attr('command');
                if (!command) {
                    return;
                }

                if (this.hasAttribute('noprompt')) {
                    AcquisitionSocket.sendCommand(context.source, command);
                    return;
                }

                let prompt = $(this).attr('prompt');
                if (!prompt) {
                    prompt = this.title;
                }

                let title = $(this).attr('prompttitle');
                if (title === undefined) {
                    title = this.textContent;
                }

                ACTION_PROMPT_DATA = {
                    'ok': function() {
                        AcquisitionSocket.sendCommand(context.source, command);
                    },
                    'title': title,
                    'details': prompt,
                };
                showModal("{{ request.url_for('static', path='/modal/actionprompt.html') }}");
            });
        }

        standardMode() {
            this.attachCollapse();
            this.attachMove();
            this.attachFormatting();
            this.attachState();
            this.attachSource();
            this.attachCommandButtons();
        }
    }


    function sortCommunicationIndicators() {
        let indicators = Array.from(communicationIndicators.childNodes);
        indicators.sort((a, b) => {
            const ia = a.textContent;
            const ib = b.textContent;
            if (ia < ib) {
                return -1;
            } else if (ia > ib) {
                return 1;
            }
            return 0;
        });
        indicators.forEach(item => communicationIndicators.appendChild(item));
    }

    class CommunicationIndicatorContext extends ListContext {
        constructor(source) {
            super(source);
        }

        attach() {
            this.attachToList(communicationIndicators);

            const button = document.createElement('button');
            button.classList.add('communication-state', 'bypass-state', 'warning-state');
            this.root.appendChild(button);

            this.attachState();

            this.addInstrumentPresent((source, info) => {
                if (!this.root) {
                    return;
                }

                const indicatorLetter = info.display_letter;
                if (!indicatorLetter || indicatorLetter === '') {
                    $(this.root).css('display', 'none');
                    return;
                }

                $(this.root).css('display', '');
                button.textContent = indicatorLetter;

                sortCommunicationIndicators();
            }, this.source);

            const context = this;
            $(button).click(function(event) {
                event.preventDefault();

                for (let i=0; i<displayContent.children.length; i++) {
                    const display = displayContent.children[i];
                    if (display.context.source !== context.source) {
                        continue;
                    }
                    display.context.activateLetter();
                }
            });
        }
    }


    function detachAllNodeContexts(rootNode) {
        Array.from(rootNode.childNodes).forEach(node => node.context.detach());
    }


    const eventLogButton = document.getElementById('show_event_log');
    const eventLogDisplay = document.getElementById('event_log_display');
    const eventLogEntries = document.getElementById('event_log_contents');
    let unacknowledgedEventAlert = false;

    const chatButton = document.getElementById('show_chat');
    const chatDisplay = document.getElementById('chat_display');
    const chatName = document.getElementById('chat_name');
    const chatText = document.getElementById('chat_text');
    const chatSend = document.getElementById('chat_send');
    const chatScrollBox = document.getElementById('chat_scroll_box');
    const chatEntries = document.getElementById('chat_contents');
    let lastSeenChat = undefined;

    function showEventLog() {
        eventLogDisplay.classList.add('visible');
        eventLogButton.classList.add('active');
        hideChat();

        eventLogDisplay.scrollTop = eventLogDisplay.scrollHeight;

        unacknowledgedEventAlert = false;
        updateEventTabel();
    }

    function hideEventLog() {
        eventLogDisplay.classList.remove('visible');
        eventLogButton.classList.remove('active');
    }

    function showChat() {
        chatDisplay.classList.add('visible');
        chatButton.classList.add('active');
        chatName.value = sessionStorage.getItem('forge-acquisition-user-name');
        setChatNew(false);
        lastSeenChat = Date.now();
        hideEventLog();
    }

    function hideChat() {
        chatDisplay.classList.remove('visible');
        chatButton.classList.remove('active');
    }

    $(eventLogButton).click(function (event) {
        event.preventDefault();

        if (eventLogDisplay.classList.contains('visible') || eventLogEntries.rows.length === 0) {
            hideEventLog();
        } else {
            showEventLog();
        }
    });
    $(chatButton).click(function (event) {
        event.preventDefault();

        lastSeenChat = Date.now();
        if (chatDisplay.classList.contains('visible')) {
            hideChat();
        } else {
            showChat();
        }
    });


    function updateEventTabel() {
        while (eventLogEntries.rows.length > 100) {
            eventLogEntries.children[0].remove();
        }

        const displayCutoff = Date.now() - 24 * 60 * 60 * 1000;
        while (eventLogEntries.rows.length > 0 && eventLogEntries.rows[0].epoch_ms < displayCutoff) {
            eventLogEntries.children[0].remove();
        }

        let displayMilliseconds = false;
        const millisecondCutoff = Date.now() - 5 * 60 * 1000;
        for (let i=eventLogEntries.rows.length-2; i>=0; i--) {
            const tr = eventLogEntries.rows[i];
            if (tr.epoch_ms < millisecondCutoff) {
                break;
            }

            const trNext = eventLogEntries.rows[i+1];
            const dT = Math.abs(tr.epoch_ms - trNext.epoch_ms);
            if (dT < 900) {
                displayMilliseconds = true;
                break;
            }
        }

        function formatTime(epoch_ms) {
            epoch_ms = Math.floor(epoch_ms);
            let date = new Date(epoch_ms);

            let formatted = date.getUTCHours().toString().padStart(2, '0') + ':' +
                date.getUTCMinutes().toString().padStart(2, '0') + ':' +
                date.getUTCSeconds().toString().padStart(2, '0');
            if (displayMilliseconds) {
                const ms = epoch_ms % 1000;
                formatted = formatted + '.' + (ms.toString().padStart(3, '0'));
            }

            return formatted;
        }

        let haveError = false;
        for (let i=0; i<eventLogEntries.rows.length; i++) {
            const tr = eventLogEntries.rows[i];

            if (tr.event.level === 'error') {
                haveError = true;
            }

            tr.children[0].textContent = formatTime(tr.epoch_ms);
        }

        if (!unacknowledgedEventAlert && !haveError) {
            unacknowledgedEventAlert = false;
        }

        if (eventLogEntries.rows.length === 0) {
            if (eventLogDisplay.classList.contains('visible')) {
                hideEventLog();
            }
            eventLogButton.classList.add('hidden');
            unacknowledgedEventAlert = false;
        } else {
            eventLogButton.classList.remove('hidden');

            if (eventLogDisplay.classList.contains('visible')) {
                unacknowledgedEventAlert = false;
            }

            if (unacknowledgedEventAlert) {
                eventLogButton.classList.remove('mdi-clipboard');
                eventLogButton.classList.add('mdi-clipboard-alert');
            } else {
                eventLogButton.classList.remove('mdi-clipboard-alert');
                eventLogButton.classList.add('mdi-clipboard');
            }
        }
    }
    CONTROL_CONTEXT.addEventLog((source, event) => {
        let epoch_ms = event.epoch_ms;
        if (!isFinite(epoch_ms)) {
            epoch_ms = Date.now();
        }

        let scrolledToBottom = (eventLogDisplay.scrollTop >= eventLogDisplay.scrollHeight - eventLogDisplay.offsetHeight);

        const tr = eventLogEntries.insertRow();
        for (let i=0; i<3; i++) {
            tr.insertCell();
        }

        tr.event = event;
        tr.epoch_ms = epoch_ms;
        tr.children[1].textContent = source;
        tr.children[2].textContent = event.message;

        if (event.level === 'error') {
            tr.classList.add('error');
            unacknowledgedEventAlert = true;
        }

        updateEventTabel();

        if (scrolledToBottom) {
            eventLogDisplay.scrollTop = eventLogDisplay.scrollHeight;
        }
    });
    updateEventTabel();
    setInterval(updateEventTabel, 60 * 1000);


    function setChatNew(has_new) {
        if (has_new) {
            chatButton.classList.add('mdi-chat-alert-outline');
            chatButton.classList.remove('mdi-chat');
        } else {
            chatButton.classList.remove('mdi-chat-alert-outline');
            chatButton.classList.add('mdi-chat');
        }
    }
    function updateChatValid() {
        let text = chatText.value;
        if (text) {
            text = text.trim();
        }

        let name = chatName.value;
        if (name) {
            name = name.trim();
        }

        if (!text || text === '') {
            chatText.classList.add('invalid');
        } else {
            chatText.classList.remove('invalid');
        }

        if (!name || name === '') {
            chatName.classList.add('invalid');
        } else {
            chatName.classList.remove('invalid');
        }

        if (!text || !name || text === '' || name === '') {
            chatSend.disabled = true;
        } else {
            chatSend.disabled = false;
        }
    }
    $(chatText).change(updateChatValid);
    $(chatText).on('input', updateChatValid);
    $(chatName).change(updateChatValid);
    $(chatName).on('input', updateChatValid);
    updateChatValid();

    function sendChatMessage() {
        let text = chatText.value;
        if (text) {
            text = text.trim();
        }

        let name = chatName.value;
        if (name) {
            name = name.trim();
        }

        if (!text || !name || text === '' || name === '') {
            return;
        }

        if (!AcquisitionSocket.sendChatMessage(name, text)) {
            return;
        }

        sessionStorage.setItem('forge-acquisition-user-name', name);
        chatText.value = '';
        updateChatValid();
    }
    $(chatText).keydown(function(event) {
        if (isModalActive()) {
            return;
        }
        if (event.target !== chatText) {
            return;
        }

        if (event.code !== 'Enter') {
            return;
        }
        if (event.shiftKey) {
            return;
        }

        event.preventDefault();
        sendChatMessage();
    });
    $(chatSend).click(function(event) {
        event.preventDefault();
        sendChatMessage();
    })

    CONTROL_CONTEXT.addChatMessage((epoch_ms, from, message) => {
        let scrolledToBottom = (chatScrollBox.scrollTop >= chatScrollBox.scrollHeight - chatScrollBox.offsetHeight);
        while (chatEntries.rows.length > 1000) {
            chatEntries.children[0].remove();
        }

        epoch_ms = Math.floor(epoch_ms);
        const date = new Date(epoch_ms);
        const displayTime = date.getUTCHours().toString().padStart(2, '0') + ':' +
            date.getUTCMinutes().toString().padStart(2, '0') + ':' +
            date.getUTCSeconds().toString().padStart(2, '0');

        const tr = chatEntries.insertRow();
        for (let i=0; i<3; i++) {
            tr.insertCell();
        }

        tr.epoch_ms = epoch_ms;
        tr.children[0].textContent = displayTime;
        tr.children[1].textContent = from;
        tr.children[2].textContent = message;
        
        if (scrolledToBottom) {
            chatScrollBox.scrollTop = chatScrollBox.scrollHeight;
        }

        if (!chatDisplay.classList.contains('visible')) {
            const now = Date.now();
            if (!isFinite(lastSeenChat) || lastSeenChat + 15 * 60 * 1000 < now) {
                if (!eventLogDisplay.classList.contains('visible')) {
                    showChat();
                    return;
                }
            }
            setChatNew(true);
        } else {
            lastSeenChat = Date.now();
        }
    });


    $('.connection-state').each(function() {
        const target = this;
        CONTROL_CONTEXT.addConnectionState((connected) => {
            if (!connected) {
                target.classList.add('disconnected');
                hideEventLog();
                hideChat();
            } else {
                target.classList.remove('disconnected');
            }
        });
    });
    $(document).keydown(function(event) {
        if (isModalActive()) {
            return;
        }
        if (event.target.matches('input') || event.target.matches('textarea')) {
            return;
        }

        for (let i=0; i<displayContent.children.length; i++) {
            const display = displayContent.children[i];
            const context = display.context;
            const hotkey = context.instrument_info.display_letter;
            if (hotkey && event.code === 'Key' + hotkey.toUpperCase()) {
                event.preventDefault();
                context.activateLetter();
            }
        }
    });

    CONTROL_CONTEXT.addConnectionState((connected) => {
        if (!connected) {
            detachAllNodeContexts(summaryContent);
            detachAllNodeContexts(displayContent);
            detachAllNodeContexts(communicationIndicators);
            hideModal();
            return;
        }

        ACQUISITION_SUMMARY_STATIC.forEach((definition) => {
            const context = new SummaryContext(allocateUID(), definition);
            context.attach();
        });

        ACQUISITION_DISPLAY_STATIC.forEach((definition) => {
            const context = new DisplayContext(allocateUID(), definition);
            context.attach();
        });
    });
    ACQUISITION_SUMMARY_INSTRUMENT.forEach((definition) => {
        CONTROL_CONTEXT.addInstrumentPresent((source, info) => {
            if (!definition.matches(source, info)) {
                return;
            }
            const context = new SummaryContext(allocateUID(), definition, source);
            context.attach();
        }, definition.source);
    });
    ACQUISITION_DISPLAY_INSTRUMENT.forEach((definition) => {
        CONTROL_CONTEXT.addInstrumentPresent((source, info) => {
            if (!definition.matches(source, info)) {
                return;
            }
            const context = new DisplayContext(allocateUID(), definition, source);
            context.attach();
        }, definition.source);
    });
    CONTROL_CONTEXT.addInstrumentPresent((source, info) => {
        if (!info.display_letter) {
            return;
        }
        const context = new CommunicationIndicatorContext(source);
        context.attach();
    });


    $('#add_message_log').click(function(e) {
        showModal("{{ request.url_for('static', path='/modal/messagelog.html') }}");
        e.preventDefault();
    });
    $('#show_restart_acquisition').click(function(e) {
        ACTION_PROMPT_DATA = {
            ok: function() {
                AcquisitionSocket.sendRestartRequest();
            },
            title: "Restart the acquisition system?",
            details: "This will restart the acquisition system, temporarily interrupting data collection.  Restarting the system is most commonly used to apply changes to the configuration.",
        };
        showModal("{{ request.url_for('static', path='/modal/actionprompt.html') }}");
        e.preventDefault();
    });
});
