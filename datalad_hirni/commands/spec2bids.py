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
import logging

lgr = logging.getLogger("datalad.hirni.spec2bids")


@build_doc
class Spec2Bids(Interface):
    """Convert to BIDS based on study specification
    """

    _params_ = dict(
        dataset=Parameter(
            args=("-d", "--dataset"),
            doc="""bids dataset""",
            constraints=EnsureDataset() | EnsureNone()),
        specfile=Parameter(
            args=("specfile",),
            metavar="SPEC_FILE",
            doc="""path(s) to the specification file(s) to use for conversion.
             If a directory at the first level beneath the dataset's root is 
             given instead of a file, it's assumed to be an acqusition directory 
             that contains a specification file.
             By default this is a file named 'studyspec.json' in the
             acquisition directory. This default name can be configured via the
             'datalad.hirni.studyspec.filename' config variable.
             """,
            nargs="*",
            constraints=EnsureStr()),
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
    def __call__(specfile, dataset=None, anonymize=False):

        dataset = require_dataset(dataset, check_installed=True,
                                  purpose="spec2bids")

        specfile = assure_list(specfile)
        specfile = [resolve_path(p, dataset) for p in specfile]

        for spec_path in specfile:
            if not lexists(spec_path):
                yield get_status_dict(
                    action='spec2bids',
                    path=spec_path,
                    status='impossible',
                    message="{} not found".format(spec_path)
                )

            if op.isdir(spec_path):
                if op.realpath(op.join(spec_path, op.pardir)) == op.realpath(dataset.path):
                    spec_path = op.join(
                            spec_path,
                            dataset.config.get("datalad.hirni.studyspec.filename",
                                               "studyspec.json")
                    )
                else:
                    yield get_status_dict(
                        action='spec2bids',
                        path=spec_path,
                        status='impossible',
                        message="{} is neither a specification file nor an "
                                "acquisition directory".format(spec_path)
                    )

            ran_heudiconv = False

            # relative path to spec to be recorded:
            rel_spec_path = relpath(spec_path, dataset.path) \
                if isabs(spec_path) else spec_path

            # check each dict (snippet) in the specification for what to do
            # wrt conversion:
            for spec_snippet in load_stream(spec_path):

                # build a dict available for placeholders in format strings:
                # Note: This is flattening the structure since we don't need
                # value/approved for the substitutions. In addition 'subject'
                # and 'anon_subject' are not passed on, but a new key
                # 'bids_subject' instead the value of which depends on the
                # --anonymize switch.
                # Additionally 'location' is recomputed to be relative to
                # dataset.path, since this is where the converters are running
                # from within.
                replacements = dict()
                for k, v in spec_snippet.items():
                    if k == 'subject':
                        if anonymize:
                            continue
                        else:
                            replacements['bids_subject'] = v['value']
                    elif k == 'anon_subject':
                        if anonymize:
                            replacements['bids_subject'] = v['value']
                        else:
                            continue
                    elif k == 'location':
                        replacements[k] = op.join(op.dirname(rel_spec_path), v)
                    elif k == 'converter_path':
                        replacements[k] = op.join(op.dirname(rel_spec_path), v['value'])
                    else:
                        replacements[k] = v['value'] if isinstance(v, dict) else v

                dataset.config.overrides = {
                    "datalad.run.substitutions._hs": replacements}
                dataset.config.reload()

                if not ran_heudiconv and \
                        heuristic.has_specval(spec_snippet, 'converter') and \
                        heuristic.get_specval(spec_snippet, 'converter') == 'heudiconv':
                    # TODO: location!

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
                    run_results = list()
                    with patch.dict('os.environ',
                                    {'HIRNI_STUDY_SPEC': rel_spec_path,
                                     'HIRNI_SPEC2BIDS_SUBJECT': replacements['bids_subject']}):

                        for r in dataset.containers_run(
                                ['heudiconv',
                                 # XXX absolute path will make rerun on other system
                                 # impossible -- hard to avoid
                                 '-f', heuristic.__file__,
                                 # leaves identifying info in run record
                                 '-s', replacements['bids_subject'],
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
                                 '--files', replacements['location']
                                 ],
                                sidecar=anonymize,
                                container_name=dataset.config.get(
                                        "datalad.hirni.conversion-container",
                                        "conversion"),
                                inputs=[replacements['location'], rel_spec_path],
                                outputs=[dataset.path],
                                message="Convert DICOM data for subject {}"
                                        "".format(replacements['bids_subject']),
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
                               'path': spec_path,
                               'snippet': spec_snippet,
                               'status': 'error',
                               'message': "acquisition conversion failed. "
                                          "See previous message(s)."}

                    else:
                        yield {'action': 'heudiconv',
                               'path': spec_path,
                               'snippet': spec_snippet,
                               'status': 'ok',
                               'message': "acquisition converted."}

                    # remove superfluous heudiconv output
                    rmtree(opj(dataset.path, rel_trash_path))
                    # run heudiconv only once
                    ran_heudiconv = True

                elif heuristic.has_specval(spec_snippet, 'converter') and \
                        heuristic.get_specval(spec_snippet, 'converter') != 'heudiconv':
                    # Spec snippet comes with a specific converter call.

                    # TODO: RF: run_converter()

                    if 'converter-container' in spec_snippet and spec_snippet['converter-container']['value']:
                        from functools import partial
                        run_cmd = partial(
                            dataset.containers_run,
                            container_name=spec_snippet['converter-container']['value']
                        )

                    else:
                        run_cmd = dataset.run

                    run_results = list()
                    for r in run_cmd(
                            spec_snippet['converter']['value'],
                            sidecar=anonymize,
                            inputs=[replacements['location'], rel_spec_path],
                            outputs=[dataset.path],
                            # Note: The following message construction is
                            # supposed to not include the acquisition identifier
                            # if --anonymize was given, since it might contain
                            # the original subject ID.
                            message="Convert {} for subject {}".format(
                                        spec_snippet['type'],
                                        replacements['bids_subject']),
                            return_type='generator',
                            #
                            ):

                        # if there was an issue with containers-run,
                        # yield original result, otherwise swallow:
                        if r['status'] not in ['ok', 'notneeded']:
                            yield r

                        run_results.append(r)

                    if not all(r['status'] in ['ok', 'notneeded']
                               for r in run_results):
                        yield {'action': 'spec2bids',
                               'path': spec_path,
                               'snippet': spec_snippet,
                               'status': 'error',
                               'message': "Conversion failed. "
                                          "See previous message(s)."}

                    else:
                        yield {'action': 'specsnippet2bids',
                               'path': spec_path,
                               'snippet': spec_snippet,
                               'status': 'ok',
                               'message': "specification converted."}

                else:
                    if heuristic.has_specval(spec_snippet, 'converter') and \
                            heuristic.get_specval(spec_snippet, 'converter') == 'heudiconv' and \
                            ran_heudiconv:
                        # in this case we acted upon this snippet already and
                        # do not have to produce a result
                        pass
                    else:
                        # no converter specified in this snippet or it's a
                        # dicomseries and heudiconv was called already
                        # => nothing to do here.
                        yield get_status_dict(
                                action='spec2bids',
                                path=spec_path,
                                snippet=spec_snippet,
                                status='notneeded',
                        )

            yield {'action': 'spec2bids',
                   'path': spec_path,
                   'status': 'ok'}


