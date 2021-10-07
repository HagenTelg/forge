var ShapeHandler = (function() {
    return class {
        constructor(div) {
            this.div = div;
            this.generators = [];
        }

        update() {
            const shapes = [];
            this.generators.forEach((gen) => {
                const add = gen();
                if (!add) {
                    return;
                }
                for (let i=0; i<add.length; i++) {
                    shapes.push(add[i]);
                }
            });
            Plotly.relayout(this.div, {
                'shapes': shapes,
            });
        }
    };
})();