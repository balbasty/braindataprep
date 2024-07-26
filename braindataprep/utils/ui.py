from typing import Literal, Tuple, Set

ByteUnitChoice = Literal['B', 'KB', 'MB', 'GB', 'TB']
ByteUnits: Set[ByteUnitChoice] = set(ByteUnitChoice.__args__)

B:  int = 1024**0
KB: int = 1024**1
MB: int = 1024**2
GB: int = 1024**3
TB: int = 1024**4
PB: int = 1024**5


def human2bytes(x: str | int) -> int:
    """
    Convert human byte size (3MB, 2GB, etc) into a number of bytes
    """
    if isinstance(x, int):
        return x
    if not isinstance(x, str):
        raise TypeError('Expected an int or a stirng')
    x = x.strip()
    unit = ''
    while x[-1] in 'ptgmkbPTGMKB':
        unit = x[-1] + unit.upper()
        x = x[:-1]
    if unit and unit[-1] == 'B':
        unit = unit[:-1]
    unit = 1024**({'': 0, 'K': 1, 'M': 2, 'G': 3, 'T': 4, 'P': 5}[unit])
    x = int(float(x.strip())*unit)
    return x


def round_bytes(x: int) -> Tuple[float, ByteUnitChoice]:
    """
    Convert a number of bytes to the unit of correct magnitude

    Parameters
    ----------
    x : int
        Number of bytes

    Returns
    -------
    x : float
        Number of [unit]
    unit : {'B', 'KB', 'MB', 'GB', 'TB', 'PB'}
        Unit of returned value
    """
    if x < KB:
        return x, 'B'
    elif x < MB:
        return x / KB, 'KB'
    elif x < GB:
        return x / MB, 'MB'
    elif x < TB:
        return x / GB, 'GB'
    elif x < PB:
        return x / TB, 'TB'
    else:
        return x / PB, 'PB'
