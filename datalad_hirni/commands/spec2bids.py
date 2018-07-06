"Convert DICOM data to BIDS based on the respective study specification"

__docformat__ = 'restructuredtext'

import os.path as op
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


# def _get_subject_from_spec(file_, anon=False):
#
#     # TODO: this is assuming an acquisition spec snippet.
#     # Need to elaborate
#     subject_key = 'subject' if not anon else 'anon_subject'
#     unique_subs = set([d[subject_key]['value']
#                        for d in load_stream(file_)
#                        if subject_key in d.keys() and d[subject_key]['value']])
#     if not unique_subs:
#         raise ValueError("missing %s in %s" % (subject_key, file_))
#     if len(unique_subs) > 1:
#         raise ValueError("subject ambiguous in %s. candidates: %s" % (file_, unique_subs))
#     return unique_subs.pop()


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
        spec_file=Parameter(
            args=("--spec-file",),
            metavar="SPEC_FILE",
            doc="""path to the specification file to use for conversion.
             By default this is a file named 'studyspec.json' in the
             acquisition directory. This default name can be configured via the
             'datalad.hirni.studyspec.filename' config variable.
             NOTE: If a relative path is given, it is interpreted as a path 
             relative to ACQUISITION_ID's dir (evaluated per acquisition). If an 
             absolute path is given, that file is used for all acquisitions to 
             be converted!""",
            constraints=EnsureStr() | EnsureNone()),
        anonymize=Parameter(
            args=("--anonymize",),
            action="store_true",
            doc="""whether or not to anonymize for conversion. By now this means
            to use 'anon_subject' instead of 'subject' from spec and to use 
            datalad-run with a sidecar file, to not leak potentially identifying 
            information into its record."""
        )
    )

    @staticmethod
    @datasetmethod(name='hirni_spec2bids')
    @eval_results
    def __call__(acquisition_id=None, dataset=None,
                 spec_file=None, anonymize=False):

        dataset = require_dataset(dataset, check_installed=True,
                                  purpose="spec2bids")

        # TODO: Be more flexible in how to specify the acquisition(visit?)
        # to be converted.
        #       Plus: Validate? (subdataset with dicoms).
        #             -> actually we can convert other data without having
        #                DICOMs available for this acquisition
        if acquisition_id is not None:
            acquisition_id = assure_list(acquisition_id)
            acquisition_id = [resolve_path(p, dataset) for p in acquisition_id]
        else:
            raise InsufficientArgumentsError(
                "insufficient arguments for spec2bids: "
                "an acquisition is required")

        if spec_file is None:
            # TODO: Use config in webapp, too. (specpath_from_id)
            spec_file = dataset.config.get("datalad.hirni.studyspec.filename",
                                           "studyspec.json")

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
                    message="Found no spec for acquisition {} at {}"
                            "".format(acq, spec_path)
                )
                # TODO: onfailure ignore?
                continue

            ran_heudiconv = False

            # relative path to spec to be recorded:
            rel_spec_path = relpath(spec_path, dataset.path) \
                if isabs(spec_path) else spec_path

            # check each dict (snippet) in the specification for what to do
            # wrt conversion:
            for spec_snippet in load_stream(spec_path):

                # build a dict available for placeholders in format strings:
                replacements = spec_snippet.copy()
                sub = replacements.pop('subject')
                anon_sub = replacements.pop('anon_subject')
                replacements['bids_subject'] = anon_sub \
                    if anonymize else sub

                if spec_snippet['type'] == 'dicomseries' and not ran_heudiconv:
                        # special treatment of DICOMs (using heudiconv)
                        # But it's one call to heudiconv for all DICOMs of an
                        # acquisition!
                        from mock import patch
                        from tempfile import mkdtemp

                        # relative path to not-needed-heudiconv output:
                        rel_trash_path = relpath(mkdtemp(prefix="hirni-tmp-",
                                                         dir=opj(dataset.path,
                                                                 ".git")),
                                                 dataset.path)
                        rel_dicom_path = relpath(opj(acq, 'dicoms'),
                                                 dataset.path)
                        run_results = list()
                        with patch.dict('os.environ',
                                        {'HIRNI_STUDY_SPEC': rel_spec_path,
                                         'HIRNI_SPEC2BIDS_SUBJECT': replacements['bids_subject']['value']}):

                            for r in dataset.containers_run(
                                    ['heudiconv',
                                     # XXX absolute path will make rerun on other system
                                     # impossible -- hard to avoid
                                     '-f', heuristic.__file__,
                                     # leaves identifying info in run record
                                     '-s', replacements['bids_subject']['value'],
                                     '-c', 'dcm2niix',
                                     # TODO decide on the fate of .heudiconv/
                                     # but ATM we need to (re)move it:
                                     # https://github.com/nipy/heudiconv/issues/196
                                     '-o', rel_trash_path,
                                     '-b',
                                     '-a', '{dspath}',
                                     '-l', '',
                                     # avoid glory details provided by dcmstack,
                                     # we have them in the aggregated DICOM
                                     # metadata already
                                     '--minmeta',
                                     '--files', rel_dicom_path
                                     ],
                                    sidecar=anonymize,
                                    container_name=dataset.config.get(
                                            "datalad.hirni.conversion-container",
                                            "conversion"),
                                    inputs=[rel_dicom_path, rel_spec_path],
                                    outputs=[dataset.path],
                                    message="Import DICOM acquisition {}".format(
                                            'for subject {}'.format(replacements['bids_subject']['value'])
                                            if anonymize else basename(acq)),
                                    return_type='generator',
                            ):
                                # if there was an issue with containers-run,
                                # yield original result, otherwise swallow:
                                if r['status'] not in ['ok', 'notneeded']:
                                    yield r

                                run_results.append(r)

                        if not all(r['status'] in ['ok', 'notneeded']
                                   for r in run_results):
                            yield {'action': 'heudiconv',
                                   'path': acq,
                                   'status': 'error',
                                   'message': "acquisition conversion failed. "
                                              "See previous message(s)."}

                        else:
                            yield {'action': 'heudiconv',
                                   'path': acq,
                                   'status': 'ok',
                                   'message': "acquisition converted."}

                        # remove superfluous heudiconv output
                        rmtree(opj(dataset.path, rel_trash_path))
                        # run heudiconv only once
                        ran_heudiconv = True

                elif spec_snippet['converter']:
                    # Spec snippet comes with a specific converter call.

                    # TODO: RF: run_converter()

                    dataset.config.overrides = {
                        "datalad.run.substitutions.hirni-spec": replacements}
                    dataset.config.reload()
                    if not spec_snippet['converter-container']:
                        run_cmd = dataset.run
                    else:
                        from functools import partial
                        run_cmd = partial(
                            dataset.containers_run,
                            container_name=spec_snippet['converter-container']
                        )

                    for r in run_cmd(
                            spec_snippet['converter'],
                            sidecar=anonymize,
                            inputs=[spec_snippet['location'], rel_spec_path],
                            outputs=[dataset.path],
                            # Note: The following message construction is
                            # supposed to not include the acquisition identifier
                            # if --anonymize was given, since it might contain
                            # the original subject ID.
                            message="Import {} from acquisition {}".format(
                                        spec_snippet['type'],
                                        'for subject {}'
                                        ''.format(replacements['bids_subject']['value'])
                                        if anonymize else basename(acq)
                            ),
                            return_type='generator',
                            #
                            ):
                        # TODO result treatment
                        pass

                else:
                    # no converter specified in this snippet or it's a
                    # dicomseries and heudiconv was called already
                    # => nothing to do here.
                    # yield a notneeded result?
                    continue

            yield {'action': 'spec2bids',
                   'path': acq,
                   'status': 'ok'}


