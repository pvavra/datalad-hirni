"""Import additional (to DICOM) data"""

from os import listdir
from os import makedirs
from os import rename
import os.path as op
from datalad.consts import ARCHIVES_SPECIAL_REMOTE
from datalad.consts import DATALAD_SPECIAL_REMOTES_UUIDS
from datalad.interface.base import build_doc, Interface
from datalad.support.constraints import EnsureStr
from datalad.support.constraints import EnsureNone
from datalad.support.constraints import EnsureKeyChoice
from datalad.support.param import Parameter
from datalad.support.network import get_local_file_url
from datalad.distribution.dataset import Dataset
from datalad.distribution.dataset import datasetmethod
from datalad.distribution.dataset import EnsureDataset
from datalad.distribution.dataset import require_dataset
from datalad.interface.utils import eval_results
from datalad.distribution.create import Create
from datalad.utils import assure_list
from datalad.support.network import RI, PathRI
from datalad.support import json_py
from datalad.coreapi import metadata

import logging
lgr = logging.getLogger('datalad.neuroimaging.import_additional_data')


def _add_to_spec(spec, path, meta):

    return spec.append({
        'type': 'additional',
        'status': None,  # TODO: process state convention; flags
        'location': op.relpath(meta['path'], path),
        'dataset_id': meta['dsid'],
        'dataset_refcommit': meta['refcommit'],
        'converter': {'value': None, 'approved': False}
    })


@build_doc
class ImportAdditionalData(Interface):
    """
    """

    _params_ = dict(
        dataset=Parameter(
            args=("-d", "--dataset"),
            metavar='PATH',
            doc="""specify the dataset to import the DICOM archive into.  If
            no dataset is given, an attempt is made to identify the dataset
            based on the current working directory and/or the `path` given""",
            constraints=EnsureDataset() | EnsureNone()),
        path=Parameter(
            args=("path",),
            metavar='PATH',
            doc="""path(s) of the data to be imported.""",
            nargs="*",
            constraints=EnsureStr()),
        acqid=Parameter(
            args=("acqid",),
            metavar="ACQUISITION ID",
            doc="""acquisition identifier for the imported data files""",
            constraints=EnsureStr() | EnsureNone()),
        target_dir=Parameter(
            args=("-t", "--target-dir"),
            metavar="TARGET",
            doc="""subdirectory of the acquisition to store the data in""",
            constraints=EnsureStr() | EnsureNone()),

    )

    @staticmethod
    @datasetmethod(name='hirni_import_data')
    @eval_results
    def __call__(acqid, path, dataset=None, target_dir=None):

        ds = require_dataset(dataset, check_installed=True,
                             purpose="import additional data")
        acq_dir = op.join(ds.path, acqid)
        if not op.exists(acq_dir):
            raise ValueError("Acquisition %s does not yet exist" % acqid)

        path = assure_list(path)

        # TODO: spec file specifiable or fixed path?
        #       if we want the former, what we actually need is an association
        #       of acquisition and its spec path
        #       => prob. not an option but a config
        spec_path = op.join(ds.path, acqid, "studyspec.json")
        spec = [r for r in json_py.load_stream(spec_path)] \
            if op.exists(spec_path) else list()

        ds_meta = ds.metadata(reporton='datasets', return_type='generator',
                              result_renderer='disabled')

        for p in path:

            src = RI(p)
            if isinstance(src, PathRI):
                src = RI("file://" + PathRI(op.abspath(p)).posixpath)  # TODO: Is this the right way for file-scheme on windows?

            dst = op.join(acq_dir, target_dir if target_dir else "",
                          op.basename(src.path))  # TODO: Again windows: If an actual URL (POSIX-path), does it still work with os.path.basename?
            lgr.debug("Adding %s to %s", src, dst)

            result = {'action': 'import additional data',
                      'type': 'file',
                      'path': dst,
                      'logger': lgr}

            if op.lexists(dst):
                yield result.update({'status': 'impossible',
                                     'message': 'file already exists'})
                return

            annex_result = ds.repo.repo.add_url_to_file(file_=dst, url=src,
                                                        unlink_existing=False)
            if not annex_result['success']:
                yield result.update({'status': 'error',
                                     'message': "annex-addurl failed: %s" %
                                                annex_result['note']})
                return

            if not target_dir:
                # TODO: This might be wrong; figure out how to do it and beware
                # that a change also need a change to support.helpers.sort_spec!

                # paths don't go in a common dir;
                # => add one spec snippet per `p`
                lgr.debug("Add specification snippet for %s", p)
                spec = _add_to_spec(spec, p, ds_meta)

        if target_dir:
            # TODO: This might be wrong; figure out how to do it and beware
            # that a change also need a change to support.helpers.sort_spec!

            # things went into a common target dir and need a common spec
            # snippet:
            lgr.debug("Add specification snippet for %s", target_dir)
            spec = _add_to_spec(spec, target_dir, ds_meta)

        from ..support.helpers import sort_spec
        spec = sorted(spec, key=lambda x: sort_spec(x))
        json_py.dump2stream(spec, spec_path)

        ds.add(spec,
               to_git=True,
               save=True,
               message="[HIRNI] Additional specification snippets for "
                       "acquisition %s" % acqid,
               return_type='generator',
               result_renderer='disabled')

        if target_dir:
            yield {'status': 'ok',
                   'action': 'import additional data',
                   'type': 'directory',
                   'path': target_dir,
                   'logger': lgr}
        else:
            for p in path:
                yield {'status': 'ok',
                       'action': 'import additional data',
                       'type': 'file',
                       'path': p,
                       'logger': lgr}
