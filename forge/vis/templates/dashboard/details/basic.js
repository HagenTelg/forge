const entry = document.getElementById('{{uid}}').entry;
const display_start_ms = "{{ start_epoch_ms }}" * 1;
const display_end_ms = "{{ end_epoch_ms }}" * 1;

$(".update-time", entry.row_details).each(function() {
    const update_time = this.getAttribute('update_time') * 1;
    this.textContent = DashboardEntry.formatFullTime(update_time);

    const now = Date.now();
    const updated_ago_ms = now - update_time;
    if (updated_ago_ms > 1000) {
        this.title = "Updated " + DashboardEntry.formatInterval(updated_ago_ms) + " ago";
    } else {
        this.title = "Updated right now";
    }
});

$(".time-offset", entry.row_details).each(function() {
    const offset_ms = Math.abs(this.getAttribute('time_offset') * 1);
    this.textContent = DashboardEntry.formatInterval(offset_ms);
});

$(".last-seen", entry.row_details).each(function() {
    this.title = "Last present: " + DashboardEntry.formatFullTime(this.getAttribute('last_seen'));
});

$(".condition-present", entry.row_details).each(function() {
    const begin_present = this.getAttribute('begin_present') * 1;
    const end_present = this.getAttribute('end_present') * 1;
    const total_ms = this.getAttribute('total_ms') * 1;
    const display = this.textContent;

    let percent = (total_ms / (display_end_ms - display_start_ms)) * 100.0;
    if (percent < 1) {
        percent = 1;
    } else if (percent > 100) {
        percent = 100;
    }

    this.textContent = display + ": " + DashboardEntry.formatInterval(total_ms) +
        " (" + percent.toFixed(0) + "%)";

    this.title = "From: " + DashboardEntry.formatFullTime(begin_present) + "\nTo: " +
        DashboardEntry.formatFullTime(end_present);
});

$(".format-date", entry.row_details).each(function() {
    const epoch_ms = Math.floor(this.getAttribute('time') * 1);
    const date = new Date(epoch_ms);
    this.textContent = date.toDateString();
    this.title = DashboardEntry.formatUTCShortDate(epoch_ms);
});
$(".format-time", entry.row_details).each(function() {
    const epoch_ms = Math.floor(this.getAttribute('time') * 1);
    this.textContent = DashboardEntry.formatLocalShortTime(epoch_ms);
    this.title = DashboardEntry.formatFullTime(epoch_ms);
});
$(".format-utc-date", entry.row_details).each(function() {
    const epoch_ms = Math.floor(this.getAttribute('time') * 1);
    const date = new Date(epoch_ms);
    this.textContent = DashboardEntry.formatUTCShortDate(epoch_ms);
    this.title = date.toDateString();
});
$(".format-utc-time", entry.row_details).each(function() {
    const epoch_ms = Math.floor(this.getAttribute('time') * 1);
    this.textContent = DashboardEntry.formatUTCShortTime(epoch_ms);
    this.title = DashboardEntry.formatFullTime(epoch_ms);
});

let priorDate = undefined;
let priorTable = undefined;
$(".hide-same-date", entry.row_details).each(function() {
    const table = this.parentNode;
    const date = $("td.format-date", this).first().text();
    if (date !== priorDate || table !== priorTable) {
        this.style.removeProperty('display');
    } else {
        this.style.display = 'none';
    }
    priorDate = date;
    priorTable = table;
});