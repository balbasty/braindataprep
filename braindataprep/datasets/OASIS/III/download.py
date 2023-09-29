import os
import click
import logging
from braindataprep.utils import get_tree_path
from braindataprep.xnat import XNAT
from .keys import allkeys, flatten_keys, compat_keys


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
        xnat.download_all_scans(
            'OASIS3', '0AS_data_files', 'OASIS3_data_files', dst=src
        )

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
                        os.path.join(src, experiment, f'{assessor}.tar.gz'),
                        type='assessor'
                    )

            if keys.intersection(compat_keys("pup")):
                assessors = xnat.get_all_assessors(
                    'OASIS3', f'OAS3{sub:04d}', experiment, '*PUPTIMECOURSE*'
                )
                for assessor in assessors:
                    assessor = assessor.split('/')[-1]

                    logging.info(f'download {experiment}/{assessor}.tar.gz')
                    xnat.download_scan(
                        'OASIS3', f'OAS3{sub:04d}', experiment, assessor,
                        os.path.join(src, experiment, f'{assessor}.tar.gz'),
                        type='assessor'
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
