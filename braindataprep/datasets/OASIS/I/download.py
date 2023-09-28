import os
import click
from braindataprep.utils import download_file, get_tree_path

URLBASE = 'https://download.nrg.wustl.edu/data'
OASISBASE = 'http://www.oasis-brains.org/files'
URLS = {
    'raw': [
        URLBASE + '/oasis_cross-sectional_disc{:d}.tar.gz'.format(d)
        for d in range(1, 13)
    ],
    'fs': [
        URLBASE + '/oasis_cs_freesurfer_disc{:d}.tar.gz'.format(d)
        for d in range(1, 13)
    ],
    'meta': f'{OASISBASE}/oasis_cross-sectional.csv',
    'reliability': f'{OASISBASE}/oasis_cross-sectional-reliability.csv',
    'facts': f'{OASISBASE}/oasis_cross-sectional_facts.pdf',
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
    path = get_tree_path(path)
    keys = set(key or ['raw', 'fs', 'meta', 'reliability', 'facts'])
    discs = set(disc or list(range(12)))
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
