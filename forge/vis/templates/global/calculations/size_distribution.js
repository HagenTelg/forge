function denormalizationFactor(Dp, binIndex) {
    const diameter = Dp[binIndex] * 1.0;
    if (!isFinite(diameter)) {
        return undefined;
    }

    if (binIndex === 0) {
        const dp1 = Dp[1] * 1.0;
        if (!isFinite(dp1) || dp1 <= 0.0) {
            return undefined;
        }
        return Math.abs(Math.log10(dp1 / diameter));
    }

    const dm1 = Dp[binIndex-1] * 1.0;
    if (binIndex === Dp.length-1) {
        if (!isFinite(dm1) || dm1 <= 0.0) {
            return undefined;
        }
        return Math.abs(Math.log10(diameter / dm1));
    }

    const dp1 = Dp[binIndex+1] * 1.0;
    if (!isFinite(dm1) || dm1 <= 0.0) {
        if (!isFinite(dp1) || dp1 <= 0.0) {
            return undefined;
        }
        return Math.abs(Math.log10(dp1 / diameter));
    }

    if (!isFinite(dp1) || dp1 <= 0.0) {
        return Math.abs(Math.log10(diameter / dm1));
    }

    const deltaDp = Math.abs(Math.sqrt(diameter * dp1) - Math.sqrt(diameter * dm1));
    return Math.LOG10E * deltaDp / diameter
}