let directives = null;
const ROOT_URL = "{{ request.url_for('root') }}";

$('#show_edit_directives').click(function(event) {
    event.preventDefault();

    if (!directives || directives.closed) {
        const directivesURL = '{{ request.url_for("editing", station=station, mode_name=mode.mode_name) }}';
        directives = window.open(directivesURL,
            'EditDirectives',
            'width=1200,height=750,menubar=0,toolbar=0,location=0,status=0,resizable=1,scrollbars=1');

        directives.onunload = function() { TimeSelect.highlight('EditDirective'); }

        TimeSelect.onZoom('EditDirective', () => {
            if (!TimeSelect.zoom_start_ms || !TimeSelect.zoom_end_ms) {
                return;
            }
            if (!directives || directives.closed || directives.location.href !== directivesURL) {
                return;
            }

            directives.postMessage({
                type: "TimeSelect",
                start_ms: TimeSelect.zoom_start_ms,
                end_ms: TimeSelect.zoom_end_ms,
            }, ROOT_URL);
        });
    } else {
        directives.focus();
    }
});

window.addEventListener("message", (event) => {
    if (event.source !== directives || !ROOT_URL.startsWith(event.origin) ||
            !event.source.location.href.startsWith(ROOT_URL)) {
        return;
    }
    const data = event.data;
    if (data.type === "EditDirectiveSelected") {
        const selectedDirective = data.directive;
        if (!selectedDirective) {
            TimeSelect.highlight('EditDirective');
            return;
        }

        TimeSelect.highlight('EditDirective',
            selectedDirective.start_epoch_ms,
            selectedDirective.end_epoch_ms,
            1);
    } else if (data.type === "EditDirectivesChanged") {
        DataSocket.reloadData();
    }
});