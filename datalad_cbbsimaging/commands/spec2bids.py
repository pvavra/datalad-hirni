__docformat__ = 'restructuredtext'

from os.path import curdir
from os.path import abspath
from os.path import join as opj
from os.path import basename

from datalad.interface.base import Interface
from datalad.interface.base import build_doc
from datalad.support.param import Parameter
from datalad.distribution.dataset import datasetmethod, EnsureDataset, \
    require_dataset, resolve_path
from datalad.interface.utils import eval_results
from datalad.support.constraints import EnsureStr
from datalad.support.constraints import EnsureNone
from datalad.support.exceptions import InsufficientArgumentsError
from datalad.coreapi import run

from datalad.interface.results import get_status_dict


def _get_subject_from_spec(file_):

    # TODO: this is assuming a session spec snippet.
    # Need to elaborate

    from datalad.support.json_py import load_stream
    unique_subs = set([d['subject']['value']
                       for d in load_stream(file_)
                       if 'subject' in d.keys()])
    if not len(unique_subs) == 1:
        raise ValueError("subject ambiguous in %s" % file_)
    return unique_subs.pop()


@build_doc
class Spec2Bids(Interface):
    """Convert to BIDS based on study specification
    """

    _params_ = dict(
            dataset=Parameter(
                    args=("-d", "--dataset"),
                    doc="""studydataset""",
                    constraints=EnsureDataset() | EnsureNone()),
            session=Parameter(
                    args=("-s", "--session",),
                    metavar="SESSION",
                    nargs="+",
                    doc="""name(s)/path(s) of the session(s) to convert.
                        like 'sourcedata/ax20_435'""",
                    constraints=EnsureStr() | EnsureNone()),
            target_dir=Parameter(
                    args=("-t", "--target-dir"),
                    doc="""Root dir of the BIDS dataset. Defaults to the root 
                    dir of the study dataset""",
                    constraints=EnsureStr() | EnsureNone()),
    )

    @staticmethod
    @datasetmethod(name='cbbs_spec2bids')
    @eval_results
    def __call__(session=None, dataset=None, target_dir=None):

        dataset = require_dataset(dataset, check_installed=True,
                                  purpose="dicoms2bids")

        from datalad.utils import assure_list, rmtree

        # TODO: Be more flexible in how to specify the session to be converted.
        #       Plus: Validate (subdataset with dicoms).
        if session is not None:
            session = assure_list(session)
            session = [resolve_path(p, dataset) for p in session]
        else:
            raise InsufficientArgumentsError(
                "insufficient arguments for spec2bids: a session is required")

        # TODO: check if target dir within dataset. (commit!)
        if target_dir is None:
            target_dir = dataset.path

        # TODO spec_file: Parameter
        spec_file = "studyspec.json"

        for ses in session:

            spec_path = opj(ses, spec_file)
            try:
                subject = _get_subject_from_spec(spec_path)
            except ValueError as e:
                yield get_status_dict(
                        action='spec2bids',
                        path=ses,
                        status='error',
                        message=str(e),
                )
                continue

            # # TODO: multi-session

            # TODO: Workaround. Couldn't pass an env variable to datalad-run:
            from mock import patch
            with patch.dict('os.environ',
                            {'CBBS_STUDY_SPEC': opj(dataset.path, spec_path)}):

                for r in dataset.run([
                    'heudiconv',
                    '-f', 'cbbs',
                    '-s', subject,
                    '-c', 'dcm2niix',
                    # TODO decide on the fate of .heudiconv/
                    # but ATM we need to (re)move it:
                    # https://github.com/nipy/heudiconv/issues/196
                    '-o', opj(dataset.path, '.git', 'stupid', basename(ses)),
                    '-b',
                    '-a', target_dir,
                    '-l', '',
                    # avoid glory details provided by dcmstack, we have them in
                    # the aggregated DICOM metadata already
                    '--minmeta',
                    '--files', opj(ses, 'dicoms')],
                        message="DICOM conversion of session {}.".format(ses)):

                    # TODO: This has to be more accurate:
                    yield r.update({'action': 'spec2bids',
                                    'path': ses})

            # remove
            rmtree(opj(dataset.path, '.git', 'stupid'))
