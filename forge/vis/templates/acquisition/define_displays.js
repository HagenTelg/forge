var ACQUISITION_DISPLAY_STATIC = [];
var ACQUISITION_DISPLAY_INSTRUMENT = [];
(function() {
    class DisplayItem {
        constructor(url) {
            this.url = url;
        }

        target(uid) {
            return this.url + "?uid=" + encodeURIComponent(uid);
        }
    }


    class StaticItem extends DisplayItem {
        constructor(url) {
            super(url);
            this.restore_key = undefined;
        }
    }

    // {% for item in mode.display_static %}
    (function() {
        let item = new StaticItem("{{ request.url_for('acquisition_display', station=station, type=item.display_type) }}");

        // {% if item.restore_key is not None %}
        item.restore_key = '{{ item.restore_key }}';
        // { %else %}
        item.restore_key = '{{ item.display_type }}';
        // {% endif %}

        ACQUISITION_DISPLAY_STATIC.push(item);
    })();
    // {% endfor %}


    class InstrumentItem extends DisplayItem {
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

    // {% for item in mode.display_instrument %}
    (function() {
        let item = new InstrumentItem("{{ request.url_for('acquisition_display', station=station, type=item.display_type) }}");

        // {% if item.match_source %}
        item.source = '{{ item.match_source }}';
        // {% endif %}
        // {% if item.match_type %}
        item.type = '{{ item.match_type }}';
        // {% endif %}

        ACQUISITION_DISPLAY_INSTRUMENT.push(item);
    })();
    // {% endfor %}
})();