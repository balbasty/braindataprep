allkeys = {
    "raw": {
        "mri": {
            "anat": {"T1w", "T2w", "TSE", "FLAIR", "T2star", "angio"},
            "func": {"pasl", "asl", "bold"},
            "": {"fmap", "dwi", "swi"}
        },
        "pet": {"fdg", "pib", "av45", "av1451"},
        "": "ct",
    },
    "derivatives": {"fs", "pup"},
    "meta": {"pheno"},
}


def flatten_keys(x, superkey=None):
    if isinstance(x, dict):
        if superkey:
            if superkey in x:
                y = set.union(
                    {superkey}, *[flatten_keys(v) for v in x.values()]
                )
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


def lower_keys(key):
    return flatten_keys(allkeys, key)


def upper_keys(key):
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


def compat_keys(key):
    return lower_keys(key).union(upper_keys(key))
