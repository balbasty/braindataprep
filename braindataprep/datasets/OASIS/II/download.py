import os
import click
from braindataprep.utils import download_file, get_tree_path

URLBASE = 'https://download.nrg.wustl.edu/data'
OASISBASE = 'http://www.oasis-brains.org/files/'
URLS = {
    'raw': [
        f'{URLBASE}/OAS2_RAW_PART1.tar.gz',
        f'{URLBASE}/OAS2_RAW_PART1.tar.gz',
    ],
    'meta': [
        f'{OASISBASE}/oasis_longitudinal_demographics.xlsx',
    ],
}


@click.command()
@click.option(
    '--path', default=None, help='Path to tree')
@click.option(
    '--packet', default=1024**2, help='Packet size for download')
@click.option(
    '--key', multiple=True, type=click.Choice(URLS.keys()),
    help='Only download these keys')
def download(path, packet, key):
    path = get_tree_path(path)
    keys = set(key or ['raw', 'fs', 'meta', 'reliability', 'facts'])
    src = os.path.join(path, 'OASIS-2', 'sourcedata')
    os.makedirs(src, exist_ok=True)
    for key in keys:
        for URL in URLS[key]:
            print('Downloading from', URL)
            download_file(
                URL, os.path.join(src, os.path.basename(URL)), packet
            )


if __name__ == '__main__':
    download()
