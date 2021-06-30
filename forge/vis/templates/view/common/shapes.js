var ShapeHandler = (function() {
    return class {
        constructor(div) {
            this.div = div;
            this.generators = [];
        }

        update() {
            const shapes = [];
            this.generators.forEach((gen) => {
                Array.prototype.push.apply(shapes, gen());
            });
            Plotly.relayout(this.div, {
                'shapes': shapes,
            });
        }
    };
})();