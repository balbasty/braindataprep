import os
import time
import requests
import logging
from pathlib import Path

from braindataprep.utils.ui import round_bytes

lg = logging.getLogger(__name__)


def download_file(src, dst=None, packet_size=1024, makedirs=True, session=None,
                  overwrite=True, **kwargs):
    """
    Download a file

    Parameters
    ----------
    src : str
        File URL.
    dst : str or Path or  file-like
        Output path.

    Other Parameters
    ----------------
    packet_size : int
        Download packets of this size.
        If None, download the entire file at once.
    makedirs : bool, default=True
        Create all directories needs to write the file
    session : requests.Session
        Pre-opened session
    overwrite : bool or 'continue'
        Which behaviour to follow if the destination file already exist.
        - True: download and overwrite
        - False: keep existing file and do not download
        - 'continue': only download and concatenate missing bytes
    logger : str or Logger
        Logger used to write things out

    Returns
    -------
    path : str
        Output path.
    """
    mode = 'wb'

    if dst is None:
        dst = Path('.') / os.path.basename(src)

    if isinstance(dst, (str, Path)):
        dst = Path(dst)
        if dst.is_dir():
            dst = dst / os.path.basename(src)
        if makedirs:
            os.makedirs(dst.parent, exist_ok=True)

        if dst.exists():
            if not overwrite:
                lg.warning(f'File already exists, skipping download: {dst}')
                return dst
            elif isinstance(overwrite, str):
                assert overwrite[0].lower() == 'c'
                mode = 'ab'

        kwargs['fname'] = dst
        with open(dst, mode) as fdst:
            return download_file(src, fdst, packet_size=packet_size,
                                 session=session, **kwargs)

    if not session:
        session = requests.Session()

    # number of bytes already written
    offset = dst.seek(0, 2)

    # total number of bytes to read
    headers = {'Range': 'bytes=0-0'}
    with session.get(src, stream=True, headers=headers) as finp:
        if finp.status_code == 206:
            total_size = int(finp.headers['Content-Range'].split('/')[1])
        else:
            total_size = None

    headers = {}

    if total_size is not None:
        total_size = int(total_size)

        if total_size == offset:
            lg.info('Nothing left to download')
            return kwargs.pop('fname', None)

        if offset:
            headers = {'Range': f'bytes={offset}-{total_size}'}

    elif offset:
        # continue mode, but cannot do it because we don't know
        # the file's expected size (and therefore how many remaining
        # bytes to read). Rerun with overwrite mode.
        return download_file(
            src, kwargs['fname'],
            packet_size=packet_size,
            makedirs=makedirs,
            session=session,
            overwrite=True,
            **kwargs
        )

    lg.info(f'download {kwargs.get("fname", "")}')

    with session.get(src, stream=True, headers=headers) as finp:
        total_size = finp.headers.get("Content-Length", None)

        if packet_size:
            packet_sum = 0
            tic = time.time()
            for packet in finp.iter_content(packet_size):
                if len(packet) == 0:
                    continue
                tac = time.time()
                dst.write(packet)
                toc = time.time()
                packet_sum += len(packet)
                show_download_progress(
                    packet_sum, total_size,
                    time=(len(packet), tic, tac, toc)
                )
                tic = time.time()
            if total_size and (packet_sum != total_size):
                print('  INCOMPLETE')
            else:
                print('  COMPLETE')
        else:
            dst.write(finp.content)
    return kwargs.pop('fname', None)


def show_download_progress(size, total_size=None, time=None, end='\r'):
    size, size_unit = round_bytes(size)
    print(f'{end}{size:7.3f} {size_unit}', end='')
    if total_size:
        total_size, total_unit = round_bytes(total_size)
        print(f' / {total_size:7.3f} {total_unit}', end='')
    if time:
        packet_size, tic, tac, toc = time
        tb, tb_unit = round_bytes(packet_size / max(toc - tic, 1e-9))
        db, db_unit = round_bytes(packet_size / max(tac - tic, 1e-9))
        wb, wb_unit = round_bytes(packet_size / max(toc - tac, 1e-9))
        print(f' [dowload: {db:7.3f} {db_unit}/s'
              f' | write: {wb:7.3f} {wb_unit}/s'
              f' | total: {tb:7.3f} {tb_unit}/s]', end='')
