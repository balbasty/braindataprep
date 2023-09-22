import os
import click
from braindataprep.utils import download_file

URLBASE = 'https://download.nrg.wustl.edu/data'
OASISBASE = 'http://www.oasis-brains.org/files/'
URLS = {
    'raw': [
        f'{URLBASE}/oasis_cross-sectional_disc1.tar.gz',
        f'{URLBASE}/oasis_cross-sectional_disc2.tar.gz',
        f'{URLBASE}/oasis_cross-sectional_disc3.tar.gz',
        f'{URLBASE}/oasis_cross-sectional_disc4.tar.gz',
        f'{URLBASE}/oasis_cross-sectional_disc5.tar.gz',
        f'{URLBASE}/oasis_cross-sectional_disc6.tar.gz',
        f'{URLBASE}/oasis_cross-sectional_disc7.tar.gz',
        f'{URLBASE}/oasis_cross-sectional_disc8.tar.gz',
        f'{URLBASE}/oasis_cross-sectional_disc9.tar.gz',
        f'{URLBASE}/oasis_cross-sectional_disc10.tar.gz',
        f'{URLBASE}/oasis_cross-sectional_disc11.tar.gz',
        f'{URLBASE}/oasis_cross-sectional_disc12.tar.gz',
    ],
    'fs': [
        f'{URLBASE}/oasis_cs_freesurfer_disc1.tar.gz',
        f'{URLBASE}/oasis_cs_freesurfer_disc2.tar.gz',
        f'{URLBASE}/oasis_cs_freesurfer_disc3.tar.gz',
        f'{URLBASE}/oasis_cs_freesurfer_disc4.tar.gz',
        f'{URLBASE}/oasis_cs_freesurfer_disc5.tar.gz',
        f'{URLBASE}/oasis_cs_freesurfer_disc6.tar.gz',
        f'{URLBASE}/oasis_cs_freesurfer_disc7.tar.gz',
        f'{URLBASE}/oasis_cs_freesurfer_disc8.tar.gz',
        f'{URLBASE}/oasis_cs_freesurfer_disc9.tar.gz',
        f'{URLBASE}/oasis_cs_freesurfer_disc10.tar.gz',
        f'{URLBASE}/oasis_cs_freesurfer_disc11.tar.gz',
        f'{URLBASE}/oasis_cs_freesurfer_disc12.tar.gz',
    ],
    'meta': f'{OASISBASE}/oasis_cross-sectional.csv',
    'reliability': f'{OASISBASE}/oasis_cross-sectional-reliability.csv',
    'facts': f'{OASISBASE}iles/oasis_cross-sectional_facts.pdf',
}


@click.command()
@click.option(
    '--path', default=None, help='Path to tree')
@click.option(
    '--packet', default=1024**2, help='Packet size for download')
@click.option(
    '--key', multiple=True, type=click.Choice(URLS.keys()),
    help='Only download these keys')
@click.option(
    '--disc', multiple=True, type=int,
    help='Only download these discs (1..12)')
def download(path, packet, key, disc):
    keys = set(key or ['raw', 'fs', 'meta', 'reliability', 'facts'])
    discs = set(disc or list(range(12)))
    path = path or '/autofs/space/pade_004/users/yb947/data4'
    src = os.path.join(path, 'OASIS-1', 'sourcedata')
    os.makedirs(src, exist_ok=True)
    for key, URL in URLS.items():
        if key not in keys:
            continue
        if key in ('raw', 'fs'):
            for disc in discs:
                URL1 = URL[disc]
                print('Downloading from', URL1)
                download_file(
                    URL1, os.path.join(src, os.path.basename(URL1)), packet
                )
        else:
            print('Downloading from', URL)
            download_file(
                URL, os.path.join(src, os.path.basename(URL)), packet
            )


if __name__ == '__main__':
    download()
