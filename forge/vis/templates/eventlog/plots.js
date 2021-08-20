const PlotInteraction = (function() {
    const ROOT_URL = "{{ request.url_for('root') }}";

    return {
        notifyEventSelected: function(event) {
            if (!window.opener) {
                return;
            }
            window.opener.postMessage({
                type: "EventLogSelected",
                event: event,
            }, ROOT_URL);
        },
    };
})();