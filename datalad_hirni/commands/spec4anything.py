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
from datalad.utils import assure_list
from datalad.interface.annotate_paths import AnnotatePaths
from datalad.interface.results import get_status_dict
from datalad.coreapi import metadata
import os.path as op

import logging
lgr = logging.getLogger('datalad.hirni.spec4anything')


def _get_edit_dict(value=None, approved=False):
    # our current concept of what an editable field looks like
    return dict(approved=approved, value=value)


def _add_to_spec(spec, spec_dir, path, meta, overrides=None):
    """
    Parameters
    ----------
    spec: list of dict
      specification to add the snippet to
    spec_dir:
      path to where the spec file is (paths in spec are relative to that location)
    path:
      path to the entity this snippet is about
    meta:
      metadata of the dataset (for dataset_id and refcommit)
    overrides: dict
      key, values to add/overwrite the default
    """

    snippet = {
        'type': 'generic_' + path['type'],
        #'status': None,  # TODO: process state convention; flags
        'location': posixpath.relpath(path['path'], spec_dir),
        'dataset_id': meta['dsid'],
        'dataset_refcommit': meta['refcommit'],
        'id': _get_edit_dict(),
        'converter': _get_edit_dict(),
        'comment': _get_edit_dict(value=""),
    }

    snippet.update(overrides)


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
    return spec


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
        spec_file=Parameter(
            args=("--spec-file",),
            metavar="SPEC_FILE",
            doc="""path to the specification file to modify.
             By default this is a file named 'studyspec.json' in the
             acquisition directory. This default name can be configured via the
             'datalad.hirni.studyspec.filename' config variable.""",
            constraints=EnsureStr() | EnsureNone()),
        properties=Parameter(
            args=("--properties",),
            metavar="PATH or JSON string",
            doc="""""",
            constraints=EnsureStr() | EnsureNone()),
    )

    @staticmethod
    @datasetmethod(name='hirni_spec4anything')
    @eval_results
    def __call__(path, dataset=None, spec_file=None, properties=None):

        dataset = require_dataset(dataset, check_installed=True,
                                  purpose="hirni spec4anything")
        path = assure_list(path)
        path = [resolve_path(p, dataset) for p in path]

        res_kwargs = dict(action='hirni spec4anything', logger=lgr)
        res_kwargs['refds'] = Interface.get_refds_path(dataset)

        ds_meta = dataset.metadata(reporton='datasets',
                                   return_type='item-or-list',
                                   result_renderer='disabled')

        # ### This might become superfluous. See datalad-gh-2653
        ds_path = PathRI(dataset.path)
        # ###

        updated_files = []
        paths = []
        for ap in AnnotatePaths.__call__(
                dataset=dataset,
                path=path,
                action='hirni spec4anything',
                unavailable_path_status='impossible',
                nondataset_path_status='error',
                return_type='generator',
                # TODO: Check this one out:
                on_failure='ignore',
                # Note/TODO: Not sure yet whether and when we need those.
                # Generally we want to be able to create a spec for subdatasets,
                # too:
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
            rel_path = posixpath.relpath(ap_path.posixpath, ds_path.posixpath)

            # TODO: This needs more generalization as we want to have higher
            # level specification snippets, that aren't within an acquisition
            path_parts = rel_path.split('/')
            if len(path_parts) < 2:
                yield get_status_dict(
                        status='error',
                        path=ap['path'],
                        message="Not within an acquisition",
                        type='file',
                        **res_kwargs
                )
                continue
            acq = path_parts[0]

            # TODO: spec file specifiable or fixed path?
            #       if we want the former, what we actually need is an
            #       association of acquisition and its spec path
            #       => prob. not an option but a config

            spec_path = spec_file if spec_file \
                else posixpath.join(ds_path.posixpath, acq,
                                    dataset.config.get("datalad.hirni.studyspec.filename",
                                                       "studyspec.json"))

            spec = [r for r in json_py.load_stream(spec_path)] \
                if posixpath.exists(spec_path) else list()

            lgr.debug("Add specification snippet for %s", ap['path'])
            # XXX 'add' does not seem to be the thing we want to do
            # rather 'set', so we have to check whether a spec for a location
            # is already known and fail or replace it (maybe with --force)

            # go through all existing specs and extract unique value
            # and also assign them to the new record (subjects, ...), but only
            # editable fields!!
            uniques = dict()
            for s in spec:
                for k in s:
                    if isinstance(s[k], dict) and 'value' in s[k]:
                        if k not in uniques:
                            uniques[k] = set()
                        uniques[k].add(s[k]['value'])
            overrides = dict()
            for k in uniques:
                if len(uniques[k]) == 1:
                    overrides[k] = _get_edit_dict(value=uniques[k].pop(),
                                                  approved=False)

            if properties:
                # load from file or json string
                props = json_py.load(properties) \
                        if op.exists(properties) else json_py.loads(properties)
                # turn into editable, pre-approved records
                props = {k: dict(value=v, approved=True) for k, v in props.items()}
                overrides.update(props)

            # TODO: It's probably wrong to use uniques for overwriting! At least
            # they cannot be used to overwrite values explicitly set in
            # _add_to_spec like "location", "type", etc.

            spec = _add_to_spec(spec, posixpath.split(spec_path)[0], ap,
                                ds_meta, overrides=overrides)

            # Note: Not sure whether we really want one commit per snippet.
            #       If not - consider:
            #       - What if we fail amidst? => Don't write to file yet.
            #       - What about input paths from different acquisitions?
            #         => store specs per acquisition in memory
            # MIH: One commit per line seems silly. why not update all files
            # collect paths of updated files, and give them to a single `add`
            # at the very end?
            # MIH: if we fail, we fail and nothing is committed
            from ..support.helpers import sort_spec
            json_py.dump2stream(sorted(spec, key=lambda x: sort_spec(x)),
                                spec_path)
            updated_files.append(spec_path)

            yield get_status_dict(
                    status='ok',
                    type=ap['type'],
                    path=ap['path'],
                    **res_kwargs)
            paths.append(ap)

        from datalad.dochelpers import single_or_plural
        from os import linesep
        message = "[HIRNI] Add specification {n_snippets} for: {paths}".format(
                n_snippets=single_or_plural("snippet", "snippets", len(paths)),
                paths=linesep.join(" - " + p['path'] for p in paths)
                if len(paths) > 1 else paths[0]['path'])
        for r in dataset.add(
                updated_files,
                to_git=True,
                save=True,
                message=message,
                return_type='generator',
                result_renderer='disabled'):
            yield r
