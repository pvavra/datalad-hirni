__docformat__ = 'restructuredtext'

import logging
from os.path import isabs
from os.path import join as opj
from os.path import basename
from os.path import lexists

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
from datalad_container import containers_run

from datalad.interface.results import get_status_dict


lgr = logging.getLogger("datalad.hirni.spec2bids")


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
        spec_file=Parameter(
            args=("--spec-file",),
            metavar="SPEC_FILE",
            doc="""path to the specification file to use for conversion.
             By default this is a file named 'studyspec.json' in the 
             session directory. NOTE: If a relative path is given, it is 
             interpreted as a path relative to session's dir (evaluated per
             session). If an absolute path is given, that file is used for all 
             sessions to be converted!""",
            constraints=EnsureStr() | EnsureNone()),
    )

    # TODO: Optional uninstall dicom ds afterwards?

    @staticmethod
    @datasetmethod(name='hirni_spec2bids')
    @eval_results
    def __call__(session=None, dataset=None, target_dir=None, spec_file=None):

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

        if spec_file is None:
            spec_file = "studyspec.json"

        for ses in session:

            if isabs(spec_file):
                spec_path = spec_file
            else:
                spec_path = opj(ses, spec_file)

            if not lexists(spec_path):
                yield get_status_dict(
                        action='spec2bids',
                        path=ses,
                        status='impossible',
                        message="Found no spec for session {}".format(ses)
                )
                # TODO: onfailure ignore?
                continue
            try:
                # TODO: AutomagicIO?
                dataset.get(spec_path)
                subject = _get_subject_from_spec(spec_path)
            except ValueError as e:
                yield get_status_dict(
                        action='spec2bids',
                        path=ses,
                        status='error',
                        message=str(e),
                )
                continue

            import datalad_hirni.support.hirni_heuristic as heuristic
            from mock import patch
            with patch.dict('os.environ',
                            {'HIRNI_STUDY_SPEC': opj(dataset.path, spec_path)}):

                # TODO: Still needed with current (container-)run?
                # Note: Workaround for datalad-run, which doesn't provide an
                # option to unlock existing output files:
                # if lexists(opj(target_dir, 'participants.tsv')):
                #     unlock = ["datalad", "unlock", "participants.tsv", ";"]
                # else:
                #     unlock = []

                for r in dataset.containers_run(
                        ['heudiconv',
                         '-f', heuristic.__file__,
                         '-s', subject,
                         '-c', 'dcm2niix',
                         # TODO decide on the fate of .heudiconv/
                         # but ATM we need to (re)move it:
                         # https://github.com/nipy/heudiconv/issues/196
                         '-o',
                         opj(dataset.path, '.git', 'stupid',
                             basename(ses)),
                         '-b',
                         '-a', target_dir,
                         '-l', '',
                         # avoid glory details provided by dcmstack, we have
                         # them in the aggregated DICOM metadata already
                         '--minmeta',
                         '--files', opj(ses, 'dicoms')
                         ],
                        container_name="conversion",  # TODO: config
                        inputs=[opj(ses, 'dicoms'), opj(dataset.path, spec_path)],
                        outputs=[target_dir],
                        message="DICOM conversion of "
                                "session {}.".format(ses),
                        return_type='generator',
                ):

                    # TODO: This isn't nice yet:
                    if r['status'] in ['ok', 'notneeded']:
                        yield {'action': 'spec2bids',
                               'path': ses,
                               'status': 'ok'}

                    else:
                        yield r
                        yield {'action': 'spec2bids',
                               'path': ses,
                               'status': 'error',
                               'message': "see above"}

                # aggregate bids and nifti metadata:
                dataset.aggregate_metadata(recursive=False,
                                           incremental=True)

            # remove
            rmtree(opj(dataset.path, '.git', 'stupid'))
