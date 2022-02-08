const PlotInteraction = (function() {
    const timeSelectedCallbacks = new Map();
    const ROOT_URL = "{{ request.url_for('root') }}";

    function sendMessageToPlot(message) {
        if (!window.opener) {
            return;
        }
        window.opener.postMessage(message, ROOT_URL);
    }

    const Interaction = {
        timeSelected: function(key, cb) {
            timeSelectedCallbacks.set(key, cb);
        },

        start_ms: undefined,
        end_ms: undefined,

        notifyDirectiveSelected: function(directive) {
            sendMessageToPlot({
                type: "EditDirectiveSelected",
                directive: directive,
            });
        },

        notifyDirectivesChanged: function() {
            sendMessageToPlot({
                type: "EditDirectivesChanged",
            });
        },
    };

    $(document).ready(function() {
        Interaction.start_ms = TimeSelect.start_ms;
        Interaction.end_ms = TimeSelect.end_ms;

        window.addEventListener("message", (event) => {
            if (event.source !== window.opener || !ROOT_URL.startsWith(event.origin) ||
                    !event.source.location.href.startsWith(ROOT_URL)) {
                return;
            }
            const data = event.data;
            if (data.type === "TimeSelect") {
                Interaction.start_ms = data.start_ms;
                Interaction.end_ms = data.end_ms;
                timeSelectedCallbacks.forEach((cb) => {
                    cb(data.start_ms, data.end_ms);
                })
            }
        });

        sendMessageToPlot({
            type: "EditDirectivesInitialize",
        });
    });

    return Interaction;
})();