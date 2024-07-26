# Adapted from `dandi.support.digest`
# Apache License Version 2.0
import hashlib
from typing import List, Mapping, Literal
from enum import IntEnum
from logging import getLogger

lg = getLogger(__name__)


DigestPriority = IntEnum('DigestPriority', [
    'md5', 'sha1', 'sha256',  'sha512',
    'sha224', 'sha384',
    'sha3_224', 'sha3_256', 'sha3_384', 'sha3_512',
    'shake_128', 'shake_256',
    'blake2b', 'blake2s',
])


def sort_digests(digests, priority=DigestPriority):
    """Sort dictionary of digests by priority"""

    if isinstance(priority, IntEnum):
        def digestsorter(x):
            return getattr(priority, x[0], float('inf'))

    elif isinstance(priority, dict):
        def digestsorter(x):
            return priority.get(x[0], float('inf'))

    else:
        priority = list(priority)

        def digestsorter(x):
            try:
                return priority.index(x[0])
            except ValueError:
                return float('inf')

    digests = {k: v for k, v in sorted(digests.items(), key=digestsorter)}
    return digests


class Digester:
    """Helper to compute multiple digests in one pass for a file"""

    # Loosely based on snippet by PM 2Ring 2014.10.23
    # http://unix.stackexchange.com/a/163769/55543

    # Ideally we should find an efficient way to parallelize this but
    # atm this one is sufficiently speedy

    def __init__(
        self,
        digests: List[str] = ('md5', 'sha1', 'sha256', 'sha512'),
        blocksize: int = 1 << 16,
        returns: Literal['digest', 'digester'] = 'digest',
    ):
        self.digests = list(digests)
        self.blocksize = blocksize
        self.returns = returns
        self.digest_funcs = [
            getattr(hashlib, digest) for digest in self.digests
        ]

    def __call__(self, fpath: str) -> Mapping[str, str]:
        """
        Parameters
        ----------
        fpath : str | Path
            File path for which a checksum shall be computed.

        Return
        ------
        dict
            Keys are algorithm labels, and values are checksum strings
        """
        lg.debug("Estimating digests for %s" % fpath)
        digests = [x() for x in self.digest_funcs]
        with open(fpath, "rb") as f:
            while True:
                block = f.read(self.blocksize)
                if not block:
                    break
                for d in digests:
                    d.update(block)
        return {
            n: d if self.returns == 'digester' else d.hexdigest()
            for n, d in zip(self.digests, digests)
        }


def get_digest(filepath: str, digest: str = "sha256") -> str:
    return Digester([digest])(filepath)[digest]


def get_digester(filepath: str, digest: str = "sha256") -> str:
    return Digester([digest], returns='digester')(filepath)[digest]
