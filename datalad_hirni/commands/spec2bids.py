"Convert DICOM data to BIDS based on the respective study specification"

__docformat__ = 'restructuredtext'


from os.path import isabs
from os.path import join as opj
from os.path import basename
from os.path import lexists
from os.path import relpath

from datalad.interface.base import Interface
from datalad.interface.base import build_doc
from datalad.support.param import Parameter
from datalad.distribution.dataset import datasetmethod
from datalad.distribution.dataset import EnsureDataset
from datalad.distribution.dataset import require_dataset
from datalad.distribution.dataset import resolve_path
from datalad.interface.results import get_status_dict
from datalad.interface.utils import eval_results
from datalad.support.constraints import EnsureStr
from datalad.support.constraints import EnsureNone
from datalad.support.exceptions import InsufficientArgumentsError
from datalad.support.json_py import load_stream
from datalad.utils import assure_list
from datalad.utils import rmtree

from datalad_container import containers_run

import datalad_hirni.support.hirni_heuristic as heuristic

# bound dataset method
import datalad.metadata.aggregate

import logging
lgr = logging.getLogger("datalad.hirni.spec2bids")


def _get_subject_from_spec(file_):

    # TODO: this is assuming a session spec snippet.
    # Need to elaborate

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
            doc="""bids dataset""",
            constraints=EnsureDataset() | EnsureNone()),
        acquisition_id=Parameter(
            args=("-a", "--acquisition-id",),
            metavar="ACQUISITION_ID",
            nargs="+",
            doc="""name(s)/path(s) of the acquisition(s) to convert.
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
             interpreted as a path relative to ACQUISITION_ID's dir (evaluated per
             acquisition). If an absolute path is given, that file is used for all
             acquisitions to be converted!""",
            constraints=EnsureStr() | EnsureNone()),
    )

    @staticmethod
    @datasetmethod(name='hirni_spec2bids')
    @eval_results
    def __call__(acquisition_id=None, dataset=None, target_dir=None, spec_file=None):

        dataset = require_dataset(dataset, check_installed=True,
                                  purpose="dicoms2bids")

        # TODO: Be more flexible in how to specify the session to be converted.
        #       Plus: Validate (subdataset with dicoms).
        if acquisition_id is not None:
            acquisition_id = assure_list(acquisition_id)
            acquisition_id = [resolve_path(p, dataset) for p in acquisition_id]
        else:
            raise InsufficientArgumentsError(
                "insufficient arguments for spec2bids: a session is required")

        # TODO: check if target dir within dataset. (commit!)
        if target_dir is None:
            target_dir = dataset.path

        if spec_file is None:
            spec_file = "studyspec.json"

        for acq in acquisition_id:

            if isabs(spec_file):
                spec_path = spec_file
            else:
                spec_path = opj(acq, spec_file)

            if not lexists(spec_path):
                yield get_status_dict(
                    action='spec2bids',
                    path=acq,
                    status='impossible',
                    message="Found no spec for session {}".format(acq)
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
                    path=acq,
                    status='error',
                    message=str(e),
                )
                continue

            from mock import patch
            # relative path to spec to be recorded:
            rel_spec_path = relpath(spec_path, dataset.path) \
                if isabs(spec_path) else spec_path

            # relative path to not-needed-heudiconv output:
            from tempfile import mkdtemp
            rel_trash_path = relpath(mkdtemp(prefix="hirni-tmp-",
                                             dir=opj(dataset.path, ".git")),
                                     dataset.path)

            rel_dicom_path = relpath(opj(acq, 'dicoms'), dataset.path)

            with patch.dict('os.environ',
                            {'HIRNI_STUDY_SPEC': rel_spec_path}):

                for r in dataset.containers_run(
                        ['heudiconv',
                         # XXX absolute path will make rerun on other system
                         # impossible -- hard to avoid
                         '-f', heuristic.__file__,
                         # leaves identifying info in run record
                         '-s', subject,
                         '-c', 'dcm2niix',
                         # TODO decide on the fate of .heudiconv/
                         # but ATM we need to (re)move it:
                         # https://github.com/nipy/heudiconv/issues/196
                         '-o', rel_trash_path,
                         '-b',
                         '-a', target_dir,
                         '-l', '',
                         # avoid glory details provided by dcmstack, we have
                         # them in the aggregated DICOM metadata already
                         '--minmeta',
                         '--files', rel_dicom_path
                         ],
                        container_name="conversion",  # TODO: config
                        inputs=[rel_dicom_path, rel_spec_path],
                        outputs=[target_dir],
                        message="DICOM conversion of "
                                "session {}.".format(basename(acq)),
                        return_type='generator',
                ):

                    # TODO: This isn't nice yet:
                    if r['status'] in ['ok', 'notneeded']:
                        yield {'action': 'spec2bids',
                               'path': acq,
                               'status': 'ok'}

                    else:
                        yield r
                        yield {'action': 'spec2bids',
                               'path': acq,
                               'status': 'error',
                               'message': "see above"}

                # aggregate bids and nifti metadata:
                dataset.aggregate_metadata(recursive=False,
                                           incremental=True)

            # remove
            rmtree(opj(dataset.path, rel_trash_path))
