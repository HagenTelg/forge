var ShapeHandler = (function() {
    return class {
        constructor(replot) {
            this._replot = replot;
            this.generators = [];

            this._replot.handlers.push(() => {
                this._applyShapes();
            });
        }

        _applyShapes() {
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

            this._replot.layout.shapes = shapes;
        }

        update(immediate) {
            this._replot.replot(immediate);
        }
    };
})();