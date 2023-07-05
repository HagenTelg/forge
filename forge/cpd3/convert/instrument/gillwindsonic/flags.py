import typing
from ..default.flags import CPD3Flag


lookup: typing.Dict[str, CPD3Flag] = {
    'insufficient_u_samples': CPD3Flag("InsufficientUAxisSamples", "Insufficient samples in average period on U axis"),
    'insufficient_v_samples': CPD3Flag("InsufficientVAxisSamples", "Insufficient samples in average period on V axis"),
    'nvm_checksum_failed': CPD3Flag("NVMChecksumFailed", "NVM checksum failed"),
    'rom_checksum_failed': CPD3Flag("ROMChecksumFailed", "ROM checksum failed"),
}
