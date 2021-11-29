import typing
from dynaconf import Dynaconf
from dynaconf.constants import DEFAULT_SETTINGS_FILES
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey as PrivateKey
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey as PublicKey

CONFIGURATION = Dynaconf(
    environments=False,
    lowercase_read=False,
    merge_enabled=True,
    default_settings_paths=DEFAULT_SETTINGS_FILES,
)


def key_to_bytes(key: typing.Union[PrivateKey, PublicKey]) -> bytes:
    from cryptography.hazmat.primitives.serialization import Encoding, PrivateFormat, PublicFormat, NoEncryption
    if isinstance(key, PrivateKey):
        return key.private_bytes(Encoding.Raw, PrivateFormat.Raw, NoEncryption())
    return key.public_bytes(Encoding.Raw, PublicFormat.Raw)
