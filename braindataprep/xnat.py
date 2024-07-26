import os
import requests
import getpass
import fnmatch
from pathlib import Path
from typing import Iterator, Iterable, Literal

from braindataprep.download import Downloader
from braindataprep.download import DownloadManager


sessions = {}
default_server = 'https://www.nitrc.org/ir/'
# It used to be https://central.xnat.org, but XNAT has been
# decomissioned in May 2024, and the platform was moved to NITRC
# I believe that the whole framework (for OASIS) is the same -- we
# just need to point to the new server and use NITRC credentials.


def get_credentials(user=None, password=None):
    user_methods = ['NITRC_USER', 'XNAT_USER', input, ValueError]
    pass_methods = ['NITRC_PASS', 'XNAT_PASS', getpass.getpass, ValueError]

    if not user:
        for method in user_methods:
            if isinstance(method, str):
                user = os.environ.get(method, None)
                if user:
                    break
            elif isinstance(method, type) and issubclass(method, Exception):
                raise ValueError(
                    'Could not guet NITRC username. '
                    'Set environment variable `NITRC_USER`'
                )
            elif callable(method):
                user = method('NITRC user: ')
                if user:
                    break

    if not password:
        for method in pass_methods:
            if isinstance(method, str):
                password = os.environ.get(method, None)
                if password:
                    break
            elif isinstance(method, type) and issubclass(method, Exception):
                raise ValueError(
                    'Could not guet NITRC password. '
                    'Set environment variable `NITRC_PASS`'
                )
            elif callable(method):
                password = getpass.getpass('NITRC password: ')
                if password:
                    break

    return user, password


class XNAT:

    # TODO:
    #   implement `keep_open` (how do I check that the session is still on?)

    def __init__(
        self,
        user: str | None = None,
        password: str | None = None,
        key: str | None = None,
        server: str | None = None,
        open: bool = False,
        keep_open: bool = True,
    ):
        sessions[key] = self
        self.credentials = get_credentials(user, password)
        self.server = server or default_server
        while self.server[-1] == '/':
            self.server = self.server[:-1]
        self.session = None
        self.jsessionid = None
        self._keep_open = None
        self._keep_open_default = keep_open
        if open:
            self.open()

    @property
    def session(self) -> requests.Session:
        if self._session is None:
            raise RuntimeError(
                'Session not open. Call `xnat.open()` or use as context '
                '`with xnat: ...`')
        return self._session

    @session.setter
    def session(self, value: requests.Session):
        self._session = value

    @property
    def keep_open(self) -> bool:
        if self._keep_open is None:
            return self._keep_open_default
        else:
            return self._keep_open

    @keep_open.setter
    def keep_open(self, value):
        self._keep_open = value

    @property
    def is_open(self) -> bool:
        return self._session is not None

    @property
    def is_closed(self) -> bool:
        return not self.is_open

    def auth(self, session: requests.Session) -> None:
        r = session.post(f'{self.server}/data/JSESSION', auth=self.credentials)
        return r.content

    def login(self) -> None:
        jsessionid = self.session.get(f'{self.server}/data/JSESSION')
        if jsessionid != self.jsessionid:
            self.jsessionid = self.auth(self.session)

    def logout(self) -> None:
        if self.is_open:
            self.session.delete(f'{self.server}/data/JSESSION')
            self.jsessionid = None

    def get(self, *args, **kwargs) -> requests.Response:
        self.login()
        return self.session.get(*args, **kwargs)

    def head(self, *args, **kwargs) -> requests.Response:
        self.login()
        return self.session.head(*args, **kwargs)

    def open(self, keep_open: bool | None = None):
        if keep_open is not None:
            self.keep_open = keep_open
        if self.is_open:
            return self
        self.session = requests.Session()
        self.login()
        return self

    def close(self):
        if self.is_closed:
            return self
        self.logout()
        self.keep_open = None
        return self

    def __enter__(self):
        self._was_open = self.is_open
        self.open()
        return self

    def __exit__(self, type, value, traceback):
        if self._was_open:
            self.close()
        delattr(self, '_was_open')
        return self

    def get_subjects(self, project: str) -> list[str]:
        """
        Parameters
        ----------
        project : str
            XNAT project name (e.g. "OASIS3")

        Returns
        -------
        subjects : list[str]
            XNAT subject label (e.g. "OAS30001")
        """
        url = f'{self.server}/data/archive/projects/{project}/subjects/'
        data = self.get(url).json()
        data = data['ResultSet']['Result']
        return [elem['label'] for elem in data]

    def get_all_subjects(
        self,
        project: str,
        subjects: str | Iterable[str] | None = None,
        **kwargs
    ) -> list[str]:
        """
        Parameters
        ----------
        project : str
            XNAT project name (e.g. "OASIS3")
        subjects : [list of] str
            Selection pattern

        Returns
        -------
        subjects : list[str]
            XNAT subject label (e.g. "OASIS3/OAS30001")
        """
        subjects = subjects or kwargs.pop('subject', None)
        subjects = filter_list(self.get_subjects(project), subjects)
        return list(map(lambda x: f'{project}/{x}', subjects))

    def get_experiments(
        self,
        project: str,
        subject: str | None = None
    ) -> list[str]:
        """
        Parameters
        ----------
        project : str
            XNAT project name (e.g. "OASIS3")
        subject : str, optional
            XNAT subject label to restrict the search to (e.g. "OAS30001")

        Returns
        -------
        experiments : list[str]
            XNAT experiments label (e.g. "OAS30001_MR_d3746")
        """
        if subject is not None:
            subject = f'/subjects/{subject}'
        else:
            subject = ''
        url = (f'{self.server}/data/archive/projects/{project}{subject}/'
               f'experiments/?format=json')
        data = self.get(url).json()['ResultSet']['Result']
        return [elem['label'] for elem in data]

    def get_all_experiments(
        self,
        project: str,
        subjects: str | Iterable[str] | None = None,
        experiments: str | Iterable[str] | None = None,
        **kwargs
    ) -> list[str]:
        """
        Parameters
        ----------
        project : str
            XNAT project name (e.g. "OASIS3")
        subject(s) : [list of] str, optional
            XNAT subject label (or selection pattern)
            to restrict the search to (e.g. "OAS30001")
        experiment(s) : [list of] str
            Selection pattern

        Returns
        -------
        experiments : list[str]
            XNAT experiments label
            (e.g. "OASIS3/OAS30001/OAS30001_MR_d3746")
        """
        subjects = subjects or kwargs.pop('subject', None)
        experiments = experiments or kwargs.pop('experiment', None)

        out = []
        subjects = self.get_all_subjects(project, subjects)
        for subject in subjects:
            proj, sub = subject.split('/')
            exp = filter_list(self.get_experiments(proj, sub), experiments)
            out.extend(map(lambda x: f'{proj}/{sub}/{x}', exp))
        return out

    def get_assessors(
        self,
        project: str,
        subject: str,
        experiment: str,
        return_info: bool = False
    ) -> list[str]:
        """
        Parameters
        ----------
        project : str
            XNAT project name (e.g. "OASIS3")
        subject : str
            XNAT subject label to restrict the search to (e.g. "OAS30001")
            If `None`, guess from experiment.
        experiment : str
            XNAT experiment label to restrict the search to
            (e.g. "OAS30001_MR_d3746")

        Returns
        -------
        scans : list[str]
            XNAT scans label (e.g. "func1")
        """
        if not subject:
            subject = self.get_subject(project, experiment)
        url = (f'{self.server}/data/archive/projects/{project}/'
               f'subjects/{subject}/experiments/{experiment}/'
               f'assessors/?format=json')
        data = self.get(url)
        if not data:
            return []
        data = data.json()['ResultSet']['Result']
        if return_info:
            return data
        else:
            return [elem['label'] for elem in data]

    def get_all_assessors(
        self,
        project: str,
        subjects: str | Iterable[str] | None = None,
        experiments: str | Iterable[str] | None = None,
        assessors: str | Iterable[str] | None = None,
        **kwargs
    ) -> list[str]:
        """
        Parameters
        ----------
        project : str
            XNAT project name (e.g. "OASIS3")
        subject(s) : [list of] str, optional
            XNAT subject label (or selection pattern)
            to restrict the search to (e.g. "OAS30001")
        experiment(s) : [list of] str
            XNAT experiment label (or selection pattern)
            to restrict the search to (e.g. "OAS30001_MR_d3746")
        assessor(s) : [list of] str
            Selection pattern

        Returns
        -------
        assessors : list[str]
            XNAT experiment + assessor label
            (e.g. "OASIS3/OAS30001/OAS30001_MR_d3746/OAS30001_Freesurfer53_d0129")
        """  # noqa: E501
        subjects = subjects or kwargs.pop('subject', None)
        experiments = experiments or kwargs.pop('experiment', None)
        assessors = assessors or kwargs.pop('assessor', None)

        out = []
        experiments = self.get_all_experiments(project, subjects, experiments)
        for experiment in experiments:
            proj, sub, exp = experiment.split('/')
            subassess = filter_list(
                self.get_assessors(proj, sub, exp), assessors
            )
            out.extend(map(lambda x: f'{proj}/{sub}/{exp}/{x}', subassess))
        return out

    def get_scans(
        self,
        project: str,
        subject: str,
        experiment: str,
        assessor: str | None = None,
        return_info=False
    ) -> list[str]:
        """
        Parameters
        ----------
        project : str
            XNAT project name (e.g. "OASIS3")
        subject : str
            XNAT subject label to restrict the search to (e.g. "OAS30001")
            If `None`, guess from experiment.
        experiment : str
            XNAT experiment label to restrict the search to
            (e.g. "OAS30001_MR_d3746")

        Returns
        -------
        scans : list[str]
            XNAT scans label (e.g. "func1")
        """
        if not subject:
            subject = self.get_subject(project, experiment)
        if assessor:
            assessor = f'assessors/{assessor}/'
        else:
            assessor = ''
        url = (f'{self.server}/data/archive/projects/{project}/'
               f'subjects/{subject}/experiments/{experiment}/{assessor}'
               f'scans/?format=json')
        data = self.get(url)
        if not data:
            return []
        data = data.json()['ResultSet']['Result']
        if return_info:
            return data
        else:
            return [elem['ID'] for elem in data]

    def get_all_scans(
        self,
        project: str,
        subjects: str | Iterable[str] | None = None,
        experiments: str | Iterable[str] | None = None,
        scans: str | Iterable[str] | None = None,
        **kwargs
    ) -> list[str]:
        """
        Parameters
        ----------
        project : str
            XNAT project name (e.g. "OASIS3")
        subject(s) : [list of] str, optional
            XNAT subject label (or selection pattern)
            to restrict the search to (e.g. "OAS30001")
        experiment(s) : [list of] str
            XNAT experiment label (or selection pattern)
            to restrict the search to (e.g. "OAS30001_MR_d3746")
        scan(s) : [list of] str
            Selection pattern

        Returns
        -------
        scans : list[str]
            XNAT experiment + scans label
            (e.g. "OASIS3/OAS30001/OAS30001_MR_d3746/func1")
        """
        subjects = subjects or kwargs.pop('subject', None)
        experiments = experiments or kwargs.pop('experiment', None)
        scans = scans or kwargs.pop('scan', None)

        out = []
        experiments = self.get_all_experiments(
            project, subjects, experiments
        )
        for experiment in experiments:
            proj, sub, exp = experiment.split('/')
            subscans = filter_list(
                self.get_scans(proj, sub, exp), scans
            )
            out.extend(map(
                lambda x: f'{proj}/{sub}/{exp}/{x}', subscans
            ))
        return out

    def get_subject(self, project: str, experiment: str):
        url = (f'{self.server}/data/archive/projects/'
               f'{project}/experiments/{experiment}/?format=json')
        data = self.session.get(url).json()
        return data['items'][0]['data_fields']['subject_ID']

    def get_downloader(
        self,
        project: str,
        subject: str,
        experiment: str,
        scan: str,
        dst: str | Path | None = None,
        *,
        type: Literal['scan', 'assessor'] = 'scan',
        **kwargs
    ) -> Downloader:
        """
        Parameters
        ----------
        project : str
            XNAT project name (e.g. "OASIS3")
        subject : str
            XNAT subject label to restrict the search to (e.g. "OAS30001")
        experiment : str
            XNAT experiment label to restrict the search to
            (e.g. "OAS30001_MR_d3746")
        scan : str
            XNAT scans label (e.g. "func1")
        dst : str or Path or file-like
            Destination

        Other Parameters
        ----------------
        type : {'scan', 'assessor'}

        Returns
        -------
        Downloader
            A downloader
        """
        if not subject:
            subject = self.get_subject(project, experiment)

        dst = dst or '.'
        if isinstance(dst, (str, Path)):
            if os.path.isdir(dst):
                dst = os.path.join(dst, experiment, f'{scan}.tar.gz')

        src = (f'{self.server}/data/archive/projects/{project}/'
               f'subjects/{subject}/experiments/{experiment}/'
               f'{type}s/{scan}/files?format=tar.gz')

        return Downloader(
            src, dst, session=self.session, auth=self.auth, **kwargs
        )

    def get_all_downloaders(
        self,
        project: str,
        subjects: str | Iterable[str] | None = None,
        experiments: str | Iterable[str] | None = None,
        scans: str | Iterable[str] | None = None,
        dst: str | Path | None = None,
        *,
        type: Literal['scan', 'assessor'] = 'scan',
        **kwargs
    ) -> Iterator[Downloader]:
        """
        Parameters
        ----------
        project : str
            XNAT project name (e.g. "OASIS3")
        subject(s) : [list of] str
            XNAT subject label (or selection pattern)
            to restrict the search to (e.g. "OAS30001")
        experiment(s) : [list of] str
            XNAT experiment label (or selection pattern)
            to restrict the search to (e.g. "OAS30001_MR_d3746")
        scan(s) : [list of] str
            XNAT scans label (or selection pattern)
            to restrict the search to (e.g. "func1")
        dst : str or Path
            Destination folder

        Returns
        -------
        yield Downloader
        """
        if 'assessor' in kwargs or 'assessors' in kwargs:
            type = 'assessor'
        subjects = subjects or kwargs.pop('subject', None)
        experiments = experiments or kwargs.pop('experiment', None)
        scans = scans or kwargs.pop(type, None) or kwargs.pop(f'{type}s', None)

        if type == 'scan':
            get_all = self.get_all_scans
        elif type == 'assessor':
            get_all = self.get_all_assessors
        else:
            raise ValueError(type)

        dst = Path(dst or '.')

        scans = get_all(project, subjects, experiments, scans)
        for scan in scans:
            proj, sub, exp, scan = scan.split('/')
            dst1 = dst / exp / f'{scan}.tar.gz'
            yield self.get_downloader(
                proj, sub, exp, scan, dst1, **kwargs, type=type
            )

    def download(self, *args, **kwargs) -> Path:
        downloader = self.get_downloader(*args, **kwargs)
        DownloadManager(downloader).run()
        return downloader.dst

    def download_all(self, *args, **kwargs) -> Path:
        downloaders = list(self.get_all_downloaders(*args, **kwargs))
        DownloadManager(*downloaders).run()
        return [x.dst for x in downloaders]


def filter_list(full_list, patterns):
    if not patterns:
        return full_list
    if isinstance(patterns, str):
        patterns = [patterns]
    elems = []
    for pattern in patterns:
        elems.extend(fnmatch.filter(full_list, pattern))
    return elems
