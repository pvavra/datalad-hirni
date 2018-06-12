"""Create specification snippets for arbitrary paths"""


import posixpath
from datalad.interface.base import build_doc, Interface
from datalad.support.constraints import EnsureStr
from datalad.support.constraints import EnsureNone
from datalad.support.param import Parameter
from datalad.distribution.dataset import resolve_path
from datalad.distribution.dataset import datasetmethod
from datalad.distribution.dataset import EnsureDataset
from datalad.distribution.dataset import require_dataset
from datalad.interface.utils import eval_results
from datalad.support.network import PathRI
from datalad.support import json_py
from datalad.interface.annotate_paths import AnnotatePaths
from datalad.interface.results import get_status_dict
from datalad.coreapi import metadata

import logging
lgr = logging.getLogger('datalad.hirni.import_additional_data')


def _add_to_spec(spec, path, meta):

    snippet = {
        'type': 'generic_' + path['type'],
        'status': None,  # TODO: process state convention; flags
        'location': path['path'],
        'dataset_id': meta['dsid'],
        'dataset_refcommit': meta['refcommit'],
        'converter': {'value': None, 'approved': False}
    }

    # TODO: if we are in an acquisition, we can get 'subject' from existing spec
    # Possibly same for other BIDS keys
    # 'bids_session',
    # 'bids_task',
    # 'bids_run',
    # 'bids_modality',
    # 'comment',
    # 'converter',
    # 'description',
    # 'id',
    # 'subject',
    spec.append(snippet)
    from ..support.helpers import sort_spec
    return sorted(spec, key=lambda x: sort_spec(x))


@build_doc
class Spec4Anything(Interface):
    """
    """

    # TODO: Allow for passing in spec values!

    _params_ = dict(
        dataset=Parameter(
            args=("-d", "--dataset"),
            metavar='PATH',
            doc="""specify the dataset. If no dataset is given, an attempt is 
            made to identify the dataset based on the current working directory 
            and/or the `path` given""",
            constraints=EnsureDataset() | EnsureNone()),
        path=Parameter(
            args=("path",),
            metavar='PATH',
            doc="""path(s) of the data to create specification for. Each path
            given will be treated as a data entity getting its own specification 
            snippet""",
            nargs="*",
            constraints=EnsureStr()),
    )

    @staticmethod
    @datasetmethod(name='hirni_import_data')
    @eval_results
    def __call__(path, dataset=None):

        dataset = require_dataset(dataset, check_installed=True,
                                  purpose="hirni spec4anything")

        res_kwargs = dict(action='hirni spec4anything', logger=lgr)
        res_kwargs['refds'] = Interface.get_refds_path(dataset)

        ds_meta = dataset.metadata(reporton='datasets',
                                   return_type='item-or-list',
                                   result_renderer='disabled')

        # ### This might become superfluous. See datalad-gh-2653
        ds_path = PathRI(dataset.path)
        # ###

        for ap in AnnotatePaths.__call__(
                dataset=dataset,
                path=path,
                action='hirni spec4anything',
                unavailable_path_status='impossible',
                nondataset_path_status='error',
                return_type='generator',
                # TODO: Check this one out:
                on_failure='ignore',
                # Note/TODO: Not sure yet whether and when we need those. Generally
                # we want to be able to create a spec for subdatasets, too:
                # recursive=recursive,
                # recursion_limit=recursion_limit,
                # force_subds_discovery=True,
                # force_parentds_discovery=True,
        ):

            if ap.get('status', None) in ['error', 'impossible']:
                yield ap
                continue

            # ### This might become superfluous. See datalad-gh-2653
            ap_path = PathRI(ap['path'])
            # ###

            # find acquisition and respective specification file:
            rel_path = resolve_path(ap_path.posixpath, ds_path.posixpath)

            # TODO: This needs more generalization as we want to have higher
            # level specification snippets, that aren't within an acquisition
            acq = rel_path.split('/')[0]
            # TODO: spec file specifiable or fixed path?
            #       if we want the former, what we actually need is an association
            #       of acquisition and its spec path
            #       => prob. not an option but a config
            spec_path = posixpath.join(ds_path.posixpath, acq, "studyspec")

            spec = [r for r in json_py.load_stream(spec_path)] \
                if posixpath.exists(spec_path) else list()

            lgr.debug("Add specification snippet for %s", ap['path'])
            spec = _add_to_spec(spec, ap, ds_meta)

            # Note: Not sure whether we really want one commit per snippet.
            #       If not - consider:
            #       - What if we fail amidst? => Don't write to file yet.
            #       - What about input paths from different acquisitions?
            #         => store specs per acquisition in memory
            json_py.dump2stream(spec, spec_path)
            dataset.add(spec,
                        to_git=True,
                        save=True,
                        message="[HIRNI] Add specification snippet for %s in "
                                "acquisition %s" % (ap['path'], acq),
                        return_type='item-or-list',
                        result_renderer='disabled')
            # TODO: Once spec snippet is actually identifiable, there should be
            # a 'notneeded' result if nothing changed. ATM it would create an
            # additional identical snippet (which is intended for now)
            yield get_status_dict(
                    status='ok',
                    type=ap['type'],
                    path=ap['path'],
                    **res_kwargs)
