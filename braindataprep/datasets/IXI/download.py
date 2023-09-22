import os
import click
from braindataprep.utils import download_file

URLBASE = 'http://biomedic.doc.ic.ac.uk/brain-development/downloads/IXI'
URLS = {
    'T1w': [f'{URLBASE}/IXI-T1.tar'],
    'T2w': [f'{URLBASE}/IXI-T2.tar'],
    'PDw': [f'{URLBASE}/IXI-PD.tar'],
    'MRA': [f'{URLBASE}/IXI-MRA.tar'],
    'DTI': [
        f'{URLBASE}/IXI-DTI.tar',
        f'{URLBASE}/bvecs.txt',
        f'{URLBASE}/bvals.txt',
    ],
    'meta': [f'{URLBASE}/IXI.xls'],
}


@click.command()
@click.option(
    '--path', default=None,
    help='Path to outer tree')
@click.option(
    '--key', multiple=True, type=click.Choice(URLS.keys()),
    help='Modalities to download')
@click.option(
    '--packet', default=1024**2,
    help='Packet size to download'
)
def download(path, key, packet):
    keys = set(key or URLS.keys())
    path = path or '/autofs/space/pade_004/users/yb947/data4'
    src = os.path.join(path, 'IXI', 'sourcedata')
    os.makedirs(src, exist_ok=True)
    for key in keys:
        for url in URLS[key]:
            print('Downloading from', url)
            download_file(
                url, os.path.join(src, os.path.basename(url)), packet
            )


if __name__ == '__main__':
    download()
