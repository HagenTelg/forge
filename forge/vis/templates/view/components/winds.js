var Winds = {};
(function() {
    Winds.DirectionWrapper = class {
        constructor() {
            this.priorDirection = undefined;
            this.priorTime = undefined;
            this.priorEpoch = undefined;
        }

        apply(direction, times, epoch) {
            const result = {
                direction: direction,
                times: times,
                epoch: epoch,
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
                const currentEpoch = result.epoch[i];
                if (!isFinite(currentDirection)) {
                    this.priorDirection = undefined;
                    this.priorTime = currentTime;
                    this.priorEpoch = currentEpoch;
                    continue;
                }

                currentDirection = (currentDirection % 360);
                result.direction[i] = currentDirection;
                if (!isFinite(this.priorDirection)) {
                    this.priorDirection = currentDirection;
                    this.priorTime = currentTime;
                    this.priorEpoch = currentEpoch;
                    continue;
                }

                const currentWrapped = applyWrap(currentDirection);
                if (Math.abs(this.priorDirection - currentDirection) <=
                        Math.abs(this.priorDirection - currentWrapped)) {
                    this.priorDirection = currentDirection;
                    this.priorTime = currentTime;
                    this.priorEpoch = currentEpoch;
                    continue;
                }

                if (needCopy) {
                    needCopy = false;
                    result.direction = direction.slice();
                    result.times = result.times.slice();
                    result.epoch = result.epoch.slice();
                }

                const priorWrapped = applyWrap(this.priorDirection);

                result.direction.splice(i, 1, currentWrapped, undefined, priorWrapped, currentDirection);
                result.times.splice(i, 1, currentTime, currentTime, this.priorTime, currentTime);
                result.epoch.splice(i, 1, currentEpoch, currentEpoch, this.priorEpoch, currentEpoch);

                this.priorDirection = currentDirection;
                this.priorTime = currentTime;
                this.priorEpoch = currentEpoch;
                i += 3;
            }

            return result;
        }
    }
})();