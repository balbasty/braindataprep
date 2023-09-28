import os
import click
import logging
from braindataprep.utils import get_tree_path
from braindataprep.xnat import XNAT


allkeys = {
    "raw": {
        "mri": {
            "anat": {"T1w", "T2w", "TSE", "FLAIR", "T2star", "angio"},
            "func": {"asl", "bold"},
            "": {"fmap", "dwi", "swi"}
        },
        "pet": {"fdg", "pib", "av45", "av1451"},
        "": "ct",
    },
    "derivatives": {"fs"},
    "": "meta",
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


meta_experiment = ('0AS_data_files', 'OASIS3_data_files')

experiment_types = {
    'AV45',
    'CT',
    'ClinicalData',
    'FDG',
    'MR',
    'PIB',
    'UDSc1',
    'USDa1',
    'USDa2',
    'USDa3',
    'USDa5',
    'USDb2',
    'USDb3',
    'USDb5',
    'USDb6',
    'USDb7',
    'USDb8',
    'USDb9',
    'USDd1',
    'data'
}
experiment_types_keep = {
    'AV45',
    'CT',
    'FDG',
    'MR',
    'PIB',
}


@click.command()
@click.option(
    '--path', default=None, help='Path to tree')
@click.option(
    '--packet', default=1024**2, help='Packet size for download')
@click.option(
    '--key', multiple=True, type=click.Choice(flatten_keys(allkeys)),
    help='Only download these keys')
@click.option(
    '--sub', multiple=True,
    help='Only download these subjects')
@click.option(
    '--user', default=None,
    help='XNAT username')
@click.option(
    '--password', default=None,
    help='XNAT password')
def download(path, packet, key, sub, user, password):
    path = get_tree_path(path)
    xnat = XNAT(user, password, open=True)
    keys = set(key or flatten_keys(allkeys))
    src = os.path.join(path, 'OASIS-3', 'sourcedata')

    print(keys)

    if 'meta' in keys:
        xnat.download_all_scans('OASIS3', *meta_experiment, dst=src)

    # get subject IDs
    subs = sub
    if not subs:
        subs = xnat.get_subjects('OASIS3')
        subs = [int(sub[4:]) for sub in subs if sub.startswith('OAS3')]
    else:
        tmp, subs = subs, []
        for sub in tmp:
            if os.path.exists(sub):
                with open(sub, 'rt') as f:
                    for line in f:
                        subs.append(int(line))
            else:
                subs.append(int(sub))

    for sub in subs:
        experiments = xnat.get_experiments('OASIS3', f'OAS3{sub:04d}')
        for experiment in experiments:

            # derivatives
            if keys.intersection(compat_keys("fs")):
                assessors = xnat.get_all_assessors(
                    'OASIS3', f'OAS3{sub:04d}', experiment, '*Freesurfer*'
                )
                for assessor in assessors:
                    assessor = assessor.split('/')[-1]

                    logging.info(f'download {experiment}/{assessor}.tar.gz')
                    xnat.download_scan(
                        'OASIS3', f'OAS3{sub:04d}', experiment, assessor,
                        os.path.join(src, experiment, f'{assessor}.tar.gz',
                                     type='assessor')
                    )

            # early filter on experiment type
            experiment_type = experiment.split('_')[1]
            if experiment_type == 'MR':
                if not keys.intersection(compat_keys("mri")):
                    continue
            elif experiment_type == "CT":
                if not keys.intersection(compat_keys("ct")):
                    continue
            elif experiment_type == "FDG":
                if not keys.intersection(compat_keys("fdg")):
                    continue
            elif experiment_type == "PIB":
                if not keys.intersection(compat_keys("pib")):
                    continue
            elif experiment_type == "AV45":
                if not keys.intersection(compat_keys("av45")):
                    continue
            else:
                continue

            scans = xnat.get_scans('OASIS3', f'OAS3{sub:04d}', experiment,
                                   return_info=True)
            for scan in scans:
                # filter on scan type (maybe not robust enough?)
                keep_scan = (
                    keys.intersection(compat_keys(scan['type'].lower())) or
                    keys.intersection(compat_keys(scan['ID'][:-1].lower()))
                )
                if keep_scan:
                    scan = scan['ID']
                    logging.info(f'download {experiment}/{scan}.tar.gz')
                    xnat.download_scan(
                        'OASIS3', f'OAS3{sub:04d}', experiment, scan,
                        os.path.join(src, experiment, f'{scan}.tar.gz')
                    )


if __name__ == '__main__':

    import sys
    root = logging.getLogger()
    root.setLevel(0)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(0)
    formatter = logging.Formatter('%(levelname)s | %(message)s')
    handler.setFormatter(formatter)
    root.addHandler(handler)

    download()
