import typing
from ..default.flags import CPD3Flag


lookup: typing.Dict[str, CPD3Flag] = {
    'too_many_particles': CPD3Flag("TooManyParticles", "Too many particles present for accurate counting due to coincidence"),
    'timing_uncertainty': CPD3Flag("TimingUncertainty", "High uncertainty in timing (10-20 microseconds)"),
}
