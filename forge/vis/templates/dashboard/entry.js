const DashboardEntry = (function() {
    const detailsLoadingQueue = new Set();
    let currentLoadingDetailsEntry = undefined;
    let currentLoadingDetailsRequest = undefined;

    function issueNextDetailsLoad() {
        const entry = detailsLoadingQueue.values().next().value;
        if (!entry) {
            return;
        }

        detailsLoadingQueue.delete(entry);
        currentLoadingDetailsEntry = entry;

        currentLoadingDetailsRequest = $.ajax({
            type: "POST",
            url: ENTRY_DETAILS_URL + "?uid=" + encodeURIComponent(entry.uid),
            timeout: 0,
            contentType: "application/json; charset=utf-8",
            data: JSON.stringify({
                code: entry.code,
                station: entry.station,
                start: entry.startTime,
            }),
        }).done(function(responseText) {
            entry.updateDetails(responseText);
        }).then(function () {
            currentLoadingDetailsRequest = undefined;
            currentLoadingDetailsEntry = undefined;
            issueNextDetailsLoad();
        });
    }

    function loadDetails(entry) {
        if (detailsLoadingQueue.has(entry)) {
            return;
        }
        if (currentLoadingDetailsEntry === entry) {
            return;
        }
        detailsLoadingQueue.add(entry);

        if (!currentLoadingDetailsRequest) {
            issueNextDetailsLoad();
        }
    }

    function cancelDetailsLoad(entry) {
        detailsLoadingQueue.delete(entry);

        if (currentLoadingDetailsEntry === entry) {
            currentLoadingDetailsRequest.abort();
        }
    }

    let uidCounter = 0;
    function allocateUID() {
        uidCounter += 1;
        return "ent" + uidCounter.toString();
    }
    
    return class {
        constructor(entryData) {
            this.uid = allocateUID();

            this.station = entryData.station.toLowerCase();
            this.code = entryData.code.toLowerCase();
            this.startTime = Date.now();
            this.sortData = [0, 0, this.station, this.code, entryData.updated_ms];
            this.emailSeverity = undefined;

            this._statusLoaded = false;
            this._detailsLoaded = false;
            this._status = new (class extends DashboardSocket.Status {
                constructor(entry) {
                    super(entry.station, entry.code);
                    this.entry = entry;
                }

                get startTime() {
                    return this.entry.startTime;
                }

                updated(data) {
                    this.entry.updateStatus(data);
                }
            })(this);

            const ref = this;

            this.row_header = document.createElement('tr');
            this.row_header.classList.add('entry');
            $(this.row_header).click(function(e) {
                e.preventDefault();
                ref._toggleDetails();
            });

            let td = document.createElement('td');
            this.row_header.appendChild(td);
            this._email_button = document.createElement('button');
            td.appendChild(this._email_button);
            this._email_button.classList.add('mdi', 'mdi-email-off', 'hidden');
            $(this._email_button).click(function(e) {
                e.preventDefault();
                e.stopPropagation();
                EMAIL_PROMPT_ENTRIES = [ref];
                showModal("{{ request.url_for('static', path='/modal/dashboardemail.html') }}");
            });

            this._status_text = document.createElement('td');
            this.row_header.appendChild(this._status_text);

            td = document.createElement('td');
            this.row_header.appendChild(td);
            this._alert_icon = document.createElement('div');
            td.appendChild(this._alert_icon);
            this._alert_icon.classList.add('mdi', 'mdi-loading', 'mdi-spin');

            this._station_text = document.createElement('td');
            this._station_text.textContent = this.station.toUpperCase();
            this.row_header.appendChild(this._station_text);

            this._code_text = document.createElement('td');
            this.row_header.appendChild(this._code_text);


            this.row_details = document.createElement('tr');
            this.row_details.classList.add('details', 'hidden');
            this.row_details.id = this.uid;
            this.row_details.entry = this;

            td = document.createElement('td');
            this.row_details.appendChild(td);
            $(td).click(function(e) {
                e.preventDefault();
                ref._toggleDetails();
            });

            this._details_content = document.createElement('td');
            this._details_content.colSpan = 4;
            this.row_details.appendChild(this._details_content);

            let loading = document.createElement('div');
            this._details_content.appendChild(loading);
            loading.classList.add('mdi', 'mdi-loading', 'mdi-spin', 'details-loading')

            this.updateData(entryData);
        }

        setUnavailable() {
            this._status.cancel();
            cancelDetailsLoad(this);
        }

        setReload() {
            this._statusLoaded = false;
            this._detailsLoaded = false;
        }

        static formatFullTime(epoch_ms) {
            epoch_ms = Math.floor(epoch_ms);
            const date = new Date(epoch_ms);

            return date.toString() + " - " + TimeParse.toDisplayTime(epoch_ms);
        }

        static formatLocalShortTime(epoch_ms) {
            epoch_ms = Math.floor(epoch_ms);
            const date = new Date(epoch_ms);

            let result = (
                date.getHours().toString().padStart(2, '0') + ':' +
                date.getMinutes().toString().padStart(2, '0') + ':' +
                date.getSeconds().toString().padStart(2, '0')
            );
            let offset = date.getTimezoneOffset() * -1;
            if (offset === 0) {
                result = result + "Z";
            } else if (offset % 60 === 0) {
                if (offset < 0) {
                    offset = offset * -1;
                    result = result + "-";
                } else {
                    result = result + "+";
                }
                result = result + Math.floor(offset / 60).toString().padStart(2, '0');
            } else {
                if (offset < 0) {
                    offset = offset * -1;
                    result = result + "-";
                } else {
                    result = result + "+";
                }
                const hours = Math.floor(offset / 60);
                offset -= hours * 60;
                result = result + "+" + hours.toString().padStart(2, '0') + offset.toString().padStart(2, '0');
            }

            return result;
        }

        static formatUTCShortDate(epoch_ms) {
            epoch_ms = Math.floor(epoch_ms);
            const date = new Date(epoch_ms);
            return (
                date.getUTCFullYear().toString().padStart(4, '0') + '-' +
                (date.getUTCMonth()+1).toString().padStart(2, '0') + '-' +
                date.getUTCDate().toString().padStart(2, '0')
            );
        }

        static formatUTCShortTime(epoch_ms) {
            epoch_ms = Math.floor(epoch_ms);
            const date = new Date(epoch_ms);
            return (
                date.getUTCHours().toString().padStart(2, '0') + ':' +
                date.getUTCMinutes().toString().padStart(2, '0') + ':' +
                date.getUTCSeconds().toString().padStart(2, '0') + 'Z'
            );
        }

        static formatInterval(interval_ms) {
            let seconds = Math.round(interval_ms / 1000);
            if (seconds < 1) {
                seconds = 1;
            }
            if (seconds < 99) {
                return seconds.toString() + "S";
            }

            const minutes = Math.floor(seconds / 60);
            if (minutes < 99) {
                return minutes.toString() + "M";
            }

            const hours = Math.floor(minutes / 60);
            if (hours < 99) {
                return hours.toString() + "H";
            }

            const days = Math.floor(hours / 24);
            return days.toString() + "D";
        }

        updateData(entryData) {
            this.row_header.classList.remove('failed', 'offline');
            this.row_details.classList.remove('failed', 'offline');

            this.sortData[4] = entryData.updated_ms;

            if (entryData.display) {
                this._code_text.textContent = entryData.display;
                this.sortData[3 ] = entryData.display;
            } else {
                this._code_text.textContent = this.code;
                this.sortData[3] = entryData.code;
            }

            function offlineForText() {
                const now = Date.now();
                const offline_ms = now - entryData.updated_ms;
                if (offline_ms < 250 || offline_ms > 999 * 24 * 60 * 60 * 1000) {
                    return "";
                }
                return " (" + DashboardEntry.formatInterval(offline_ms) + ")";
            }

            switch (entryData.status) {
            case 'failed':
                this._status_text.textContent = "FAILED";
                this._status_text.title = "Failure detected at " + DashboardEntry.formatFullTime(entryData.updated_ms);
                this.row_header.classList.add('failed');
                this.row_details.classList.add('failed');
                this.sortData[0] = -2;
                break;
            case 'offline':
                this._status_text.textContent = "OFFLINE" + offlineForText();
                this._status_text.title = "No report since " + DashboardEntry.formatFullTime(entryData.updated_ms);
                this.row_header.classList.add('offline');
                this.row_details.classList.add('offline');
                this.sortData[0] = -1;
                break;
            case 'offline_failed':
                this._status_text.textContent = "OFFLINE" + offlineForText();
                this._status_text.title = "Failure detected at " + DashboardEntry.formatFullTime(entryData.updated_ms);
                this.row_header.classList.add('offline', 'failed');
                this.row_details.classList.add('offline', 'failed');
                this.sortData[0] = -3;
                break;
            default:
                this._status_text.textContent = "OK";
                this._status_text.title = "Status nominal, last updated " + DashboardEntry.formatFullTime(entryData.updated_ms);
                this.sortData[0] = 0;
                break;
            }
        }

        updateStartTime(startTime) {
            if (this.startTime === startTime) {
                return;
            }

            this.startTime = startTime;
            this._statusLoaded = false;
            this._detailsLoaded = false;
        }

        updateStatus(statusData) {
            this._statusLoaded = true;
            let sortChanged = false;

            this._alert_icon.classList.remove('mdi-loading', 'mdi-spin',
                'mdi-alert', 'mdi-message-alert', 'mdi-message', 'hidden');
            switch (statusData.information) {
            case 'error':
                this._alert_icon.classList.add('mdi-alert');
                this._alert_icon.title = "Error";
                if (this.sortData[1] !== -3) {
                    sortChanged = true;
                    this.sortData[1] = -3;
                }
                break;
            case 'warning':
                this._alert_icon.classList.add('mdi-message-alert');
                this._alert_icon.title = "Warning";
                if (this.sortData[1] !== -2) {
                    sortChanged = true;
                    this.sortData[1] = -2;
                }
                break;
            case 'info':
                this._alert_icon.classList.add('mdi-message');
                this._alert_icon.title = "Additional information";
                if (this.sortData[1] !== -1) {
                    sortChanged = true;
                    this.sortData[1] = -1;
                }
                break;
            default:
                this._alert_icon.classList.add('hidden');
                if (this.sortData[1] !== 0) {
                    sortChanged = true;
                    this.sortData[1] = 0;
                }
                break;
            }

            this.emailSeverity = statusData.email;
            this._email_button.classList.remove('mdi-email', 'mdi-email-off', 'error', 'warning');
            switch (statusData.email) {
            case 'error':
                this._email_button.classList.add('mdi-email', 'error');
                this._email_button.title = "Emails are sent on errors only";
                break;
            case 'warning':
                this._email_button.classList.add('mdi-email', 'warning');
                this._email_button.title = "Emails are sent on errors and warnings";
                break;
            case 'info':
                this._email_button.classList.add('mdi-email', 'info');
                this._email_button.title = "Emails are sent when any message is present";
                break;
            case 'always':
                this._email_button.classList.add('mdi-email');
                this._email_button.title = "Emails are always sent";
                break;
            default:
                this._email_button.classList.add('mdi-email-off');
                this._email_button.title = "Emails disabled";
                break;
            }
            //{% if (enable_user_actions and request.user.auth_user.email) or is_example %}
            this._email_button.classList.remove('hidden');
            //{% endif %}


            if (sortChanged) {
                Sorting.deferUpdateTable();
            }
        }

        updateVisibility(visible) {
            if (!visible) {
                detailsLoadingQueue.delete(this);
                return;
            }

            if (!this._statusLoaded) {
                this._status.load();
            }
        }

        _toggleDetails() {
            if (!this.row_details.classList.contains('hidden')) {
                this.row_details.classList.add('hidden');
                detailsLoadingQueue.delete(this);
                return;
            }

            this.row_details.classList.remove('hidden');
            if (!this._detailsLoaded) {
                loadDetails(this);
            }
        }

        updateDetails(responseText) {
            this._detailsLoaded = true;
            $(this._details_content).html(responseText);
        }

        emailChanged() {
            this._status.load();
        }
    };
})();