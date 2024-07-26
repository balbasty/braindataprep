allkeys: dict = {
    "raw": {
        "mri": {
            "anat": {"T1w", "T2w", "TSE", "FLAIR", "T2star", "angio", "swi"},
            "func": {"bold"},
            "perf": {"pasl", "asl"},
            "": {"fmap", "fieldmap", "dwi"}
        },
        "pet": {"FDG", "PIB", "AV45", "AV1451"},
        "ct": {"CT"},
    },
    "derivatives": {"fs", "fs-all", "pup"},
    "meta": {"pheno"},
}


def _get_leaves(obj=allkeys) -> set[str]:
    if isinstance(obj, set):
        return obj
    if isinstance(obj, str):
        return {obj}
    return set().union(*map(_get_leaves, obj.values()))


allleaves: set[str] = _get_leaves()


def flatten_keys(x, superkey: str | None = None) -> set[str]:
    """
    Return the set of all keys.

    If `superkey`, only keys that are below this super-key are inluded.
    """
    if isinstance(x, dict):
        if superkey:
            if superkey in x:
                y = {superkey} | flatten_keys(x[superkey])
            else:
                y = set.union(
                    *[flatten_keys(v, superkey) for v in x.values()]
                )
        else:
            y = set.union(
                set(x.keys()), *[flatten_keys(v) for v in x.values()]
            )
    else:
        if isinstance(x, str):
            x = {x}
        assert isinstance(x, set)
        if superkey:
            y = x.intersection({superkey})
        else:
            y = set(x)
    y.discard("")
    return y


def lower_keys(key: str) -> set[str]:
    """Return all keys t:hat are below `key` in the hierarchy"""
    return flatten_keys(allkeys, key)


def upper_keys(key: str) -> set[str]:
    """Return all keys that are above `key` in the hierarchy"""
    def _impl(x):
        if isinstance(x, dict):
            if key in x.keys():
                return {key}
            else:
                keys = set()
                for k, v in x.items():
                    v = _impl(v)
                    if v:
                        keys = keys.union({k}, v)
                return keys
        else:
            if isinstance(x, str):
                x = {x}
            assert isinstance(x, set)
            if key in x:
                return {key}
            else:
                return set()
    keys = _impl(allkeys)
    keys.discard("")
    return keys


def compat_keys(key: str) -> set[str]:
    """Return all keys that are compatible with `key`"""
    return lower_keys(key).union(upper_keys(key))


def lower_equal_key(x: str, y: str) -> bool:
    return x in lower_keys(y)


def lower_key(x: str, y: str) -> bool:
    return x != y and x in lower_keys(y)


def upper_equal_key(x: str, y: str) -> bool:
    return x in upper_keys(y)


def upper_key(x: str, y: str) -> bool:
    return x != y and x in upper_keys(y)
