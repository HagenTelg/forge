var Winds = {};
(function() {
    Winds.DirectionWrapper = class {
        constructor() {
            this.priorDirection = undefined;
            this.priorTime = undefined;
        }

        apply(direction, times) {
            const result = {
                direction: direction,
                times: times,
            };
            let needCopy = true;

            function applyWrap(value) {
                if (value < 180.0) {
                    return value + 360.0;
                } else {
                    return value - 360.0;
                }
            }

            for (let i=0; i<result.direction.length; i++) {
                let currentDirection = result.direction[i];
                const currentTime = result.times[i];
                if (!isFinite(currentDirection)) {
                    this.priorDirection = undefined;
                    this.priorTime = currentTime;
                    continue;
                }

                currentDirection = (currentDirection % 360);
                result.direction[i] = currentDirection;
                if (!isFinite(this.priorDirection)) {
                    this.priorDirection = currentDirection;
                    this.priorTime = currentTime;
                    continue;
                }

                const currentWrapped = applyWrap(currentDirection);
                if (Math.abs(this.priorDirection - currentDirection) <=
                        Math.abs(this.priorDirection - currentWrapped)) {
                    this.priorDirection = currentDirection;
                    this.priorTime = currentTime;
                    continue;
                }

                if (needCopy) {
                    needCopy = false;
                    result.direction = direction.slice();
                    result.times = result.times.slice();
                }

                const priorWrapped = applyWrap(this.priorDirection);

                result.direction.splice(i, 1, currentWrapped, undefined, priorWrapped, currentDirection);
                result.times.splice(i, 1, currentTime, currentTime, this.priorTime, currentTime);

                this.priorDirection = currentDirection;
                this.priorTime = currentTime;
                i += 3;
            }

            return result;
        }
    }
})();