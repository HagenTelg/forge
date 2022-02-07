var ACQUISITION_SUMMARY_STATIC = [];
var ACQUISITION_SUMMARY_INSTRUMENT = [];
(function() {
    class SummaryItem {
        constructor(url) {
            this.url = url;
            this.priority = 0;
        }

        target(uid) {
            return this.url + "?uid=" + encodeURIComponent(uid);
        }
    }


    class StaticItem extends SummaryItem {
        constructor(url) {
            super(url);
        }
    }

    // {% for item in mode.summary_static %}
    (function() {
        let item = new StaticItem("{{ request.url_for('acquisition_summary', station=station, type=item.summary_type) }}");

        // {% if item.priority %}
        item.priority = '{{ item.priority }}';
        // {% endif %}

        ACQUISITION_SUMMARY_STATIC.push(item);
    })();
    // {% endfor %}


    class InstrumentItem extends SummaryItem {
        constructor(url) {
            super(url);
            this.source = undefined;
            this.type = undefined;
        }

        target(uid, source) {
            let url = super.target(uid);
            if (source) {
                url = url + "&source=" + encodeURIComponent(source);
            }
            return url;
        }

        matches(source, info) {
            if (this.source && this.source !== source) {
                return false;
            }
            if (this.type && this.type !== info.type) {
                return false;
            }

            return true;
        }
    }

    // {% for item in mode.summary_instrument %}
    (function() {
        let item = new InstrumentItem("{{ request.url_for('acquisition_summary', station=station, type=item.summary_type) }}");

        // {% if item.priority %}
        item.priority = '{{ item.priority }}';
        // {% endif %}

        // {% if item.match_source %}
        item.source = '{{ item.match_source }}';
        // {% endif %}
        // {% if item.match_type %}
        item.type = '{{ item.match_type }}';
        // {% endif %}

        ACQUISITION_SUMMARY_INSTRUMENT.push(item);
    })();
    // {% endfor %}
})();