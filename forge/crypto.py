import typing
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey as PrivateKey, Ed25519PublicKey as PublicKey


def key_to_bytes(key: typing.Union[PrivateKey, PublicKey]) -> bytes:
    from cryptography.hazmat.primitives.serialization import Encoding, PrivateFormat, PublicFormat, NoEncryption
    if isinstance(key, PrivateKey):
        return key.private_bytes(Encoding.Raw, PrivateFormat.Raw, NoEncryption())
    return key.public_bytes(Encoding.Raw, PublicFormat.Raw)
