import requests
import getpass
import fnmatch
import os
from pathlib import Path
from .utils import download_file


sessions = {}
default_server = 'https://central.xnat.org'


def get_credentials(user=None, password=None):
    if user:
        if not password:
            password = getpass.getpass()
        return user, password

    user = os.environ.get('XNAT_USER', None)
    if not user:
        raise ValueError(
            'Could not guet XNAT username. Set environment variable `XNAT_USER'
        )
    password = os.environ.get('XNAT_PASS', None)
    if not user:
        raise ValueError(
            'Could not guet XNAT password. Set environment variable `XNAT_PASS'
        )
    return user, password


class XNAT:

    # TODO:
    #   implement `keep_open` (how do I check that the session is still on?)

    def __init__(self, user=None, password=None, key=None, server=None,
                 open=False, keep_open=True):
        sessions[key] = self
        self.auth = get_credentials(user, password)
        self.server = server or default_server
        while self.server[-1] == '/':
            self.server = self.server[:-1]
        self.session = None
        self._keep_open = None
        self._keep_open_default = keep_open
        if open:
            self.open()

    @property
    def session(self):
        if self._session is None:
            raise RuntimeError(
                'Session not open. Call `xnat.open()` or use as context '
                '`with xnat: ...`')
        return self._session

    @session.setter
    def session(self, value):
        self._session = value

    @property
    def keep_open(self):
        if self._keep_open is None:
            return self._keep_open_default
        else:
            return self._keep_open

    @keep_open.setter
    def keep_open(self, value):
        self._keep_open = value

    @property
    def is_open(self):
        return self._session is not None

    @property
    def is_closed(self):
        return not self.is_open

    def open(self, keep_open=None):
        if keep_open is not None:
            self.keep_open = keep_open
        if self.is_open:
            return self
        self.session = requests.Session()
        self.session.post(f'{self.server}/data/JSESSION', auth=self.auth)
        return self

    def close(self):
        if self.is_closed:
            return self
        self.session.delete(f'{self.server}/data/JSESSION')
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

    def get_subjects(self, project):
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
        data = self.session.get(url).json()['ResultSet']['Result']
        return [elem['label'] for elem in data]

    def get_all_subjects(self, project, subjects=None, **kwargs):
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

    def get_experiments(self, project, subject=None):
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
        data = self.session.get(url).json()['ResultSet']['Result']
        return [elem['label'] for elem in data]

    def get_all_experiments(self, project, subjects=None, experiments=None,
                            **kwargs):
        """
        Parameters
        ----------
        project : str
            XNAT project name (e.g. "OASIS3")
        subject : [list of] str, optional
            XNAT subject label (or selection pattern)
            to restrict the search to (e.g. "OAS30001")
        experiments : [list of] str
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

    def get_assessors(self, project, subject, experiment, return_info=False):
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
        data = self.session.get(url)
        if not data:
            return []
        data = data.json()['ResultSet']['Result']
        if return_info:
            return data
        else:
            return [elem['label'] for elem in data]

    def get_all_assessors(
            self, project, subjects=None, experiments=None, assessors=None,
            **kwargs):
        """
        Parameters
        ----------
        project : str
            XNAT project name (e.g. "OASIS3")
        subject : [list of] str, optional
            XNAT subject label (or selection pattern)
            to restrict the search to (e.g. "OAS30001")
        experiments : [list of] str
            XNAT experiment label (or selection pattern)
            to restrict the search to (e.g. "OAS30001_MR_d3746")
        assessors : [list of] str
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

    def get_scans(self, project, subject, experiment, assessors=None,
                  return_info=False):
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
        if assessors:
            assessors = f'assessors/{assessors}/'
        else:
            assessors = ''
        url = (f'{self.server}/data/archive/projects/{project}/'
               f'subjects/{subject}/experiments/{experiment}/{assessors}'
               f'scans/?format=json')
        data = self.session.get(url)
        if not data:
            return []
        data = data.json()['ResultSet']['Result']
        if return_info:
            return data
        else:
            return [elem['ID'] for elem in data]

    def get_all_scans(
            self, project, subjects=None, experiments=None, scans=None,
            **kwargs):
        """
        Parameters
        ----------
        project : str
            XNAT project name (e.g. "OASIS3")
        subject : [list of] str, optional
            XNAT subject label (or selection pattern)
            to restrict the search to (e.g. "OAS30001")
        experiments : [list of] str
            XNAT experiment label (or selection pattern)
            to restrict the search to (e.g. "OAS30001_MR_d3746")
        scans : [list of] str
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

    def get_subject(self, project, experiment):
        url = (f'{self.server}/data/archive/projects/'
               f'{project}/experiments/{experiment}/?format=json')
        data = self.session.get(url).json()
        return data['items'][0]['data_fields']['subject_ID']

    def download_scan(self, project, subject, experiment, scan, dst=None,
                      *, type='scan', **kwargs):
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
        dst : str
            Path to output file
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

        return download_file(src, dst, session=self.session, **kwargs)

    def download_all_scans(
            self, project, subjects=None, experiments=None, scans=None,
            dst=None, *, type='scan', **kwargs):
        """
        Parameters
        ----------
        project : str
            XNAT project name (e.g. "OASIS3")
        subjects : [list of] str
            XNAT subject label (or selection pattern)
            to restrict the search to (e.g. "OAS30001")
        experiments : [list of] str
            XNAT experiment label (or selection pattern)
            to restrict the search to (e.g. "OAS30001_MR_d3746")
        scans : [list of] str
            XNAT scans label (or selection pattern)
            to restrict the search to (e.g. "func1")
        dst : str or Path
            Destination folder

        Returns
        -------
        dst : list[str]
            Path to output files
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

        ofiles = []
        scans = get_all(project, subjects, experiments, scans)
        for scan in scans:
            proj, sub, exp, scan = scan.split('/')
            dst1 = os.path.join(dst, exp, f'{scan}.tar.gz')
            ofiles.append(self.download_scan(
                proj, sub, exp, scan, dst1, **kwargs, type=type
            ))
        return ofiles


def filter_list(full_list, patterns):
    if not patterns:
        return full_list
    if isinstance(patterns, str):
        patterns = [patterns]
    elems = []
    for pattern in patterns:
        elems.extend(fnmatch.filter(full_list, pattern))
    return elems
