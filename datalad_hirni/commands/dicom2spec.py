"""Derive a study specification snippet describing a DICOM series based on the
DICOM metadata as provided by datalad.
"""

import logging
import os.path as op

from datalad.coreapi import metadata
from datalad.distribution.dataset import EnsureDataset
from datalad.distribution.dataset import datasetmethod
from datalad.distribution.dataset import require_dataset
from datalad.distribution.dataset import resolve_path
from datalad.interface.base import Interface
from datalad.interface.base import build_doc
from datalad.interface.common_opts import recursion_flag
from datalad.interface.utils import eval_results
from datalad.support import json_py
from datalad.support.constraints import EnsureNone
from datalad.support.constraints import EnsureStr
from datalad.support.exceptions import InsufficientArgumentsError
from datalad.support.param import Parameter

lgr = logging.getLogger('datalad.hirni.dicom2spec')

# ############################# Build plugin mechanism for Rules finally!
#########################################


def add_to_spec(ds_metadata, spec_list, basepath,
                subject=None, anon_subject=None, session=None, overrides=None):

    from datalad_hirni.support.dicom2bids_rules import \
        get_rules_from_metadata, series_is_valid  # TODO: RF?

    lgr.debug("Discovered %s image series.",
              len(ds_metadata['metadata']['dicom']['Series']))

    # generate a list of dicts, with the "rule-proof" entries:
    base_list = []
    for series in ds_metadata['metadata']['dicom']['Series']:
        base_list.append({
            # Note: The first 4 entries aren't a dict and have no
            # "approved flag", since they are automatically managed
            'type': 'dicomseries',
            #'status': None,  # TODO: process state convention; flags
            'location': op.relpath(ds_metadata['path'], basepath),
            'uid': series['SeriesInstanceUID'],
            'dataset_id': ds_metadata['dsid'],
            'dataset_refcommit': ds_metadata['refcommit'],
            'converter': {
                'value': 'heudiconv' if series_is_valid(series) else 'ignore',
                # TODO: not clear yet, what exactly to specify here
                'approved': False},
        })

    # get rules to apply:
    rules = get_rules_from_metadata(
            ds_metadata['metadata']['dicom']['Series'])
    for rule_cls in rules:
        rule = rule_cls(ds_metadata['metadata']['dicom']['Series'])
        for idx, values in zip(range(len(base_list)),
                               rule(subject=subject,
                                    anon_subject=anon_subject,
                                    session=session)
                               ):
            for k in values.keys():
                base_list[idx][k] = {'value': values[k],
                                     'approved': False}

    # merge with existing spec plus overrides:
    for series in base_list:

        series.update(overrides)

        existing = [i for s, i in
                    zip(spec_list, range(len(spec_list)))
                    if s['uid'] == series['uid']]
        if existing:
            lgr.debug("Updating existing spec for image series %s",
                      series['uid'])
            # we already had data of that series in the spec;
            spec_list[existing[0]].update(series)
        else:
            lgr.debug("Creating spec for image series %s", series['uid'])
            spec_list.append(series)

    return spec_list


@build_doc
class Dicom2Spec(Interface):
    """Derives a specification snippet from DICOM metadata and stores it in a
    JSON file
    """

    _params_ = dict(
            dataset=Parameter(
                    args=("-d", "--dataset"),
                    doc="""specify a dataset containing the DICOM metadata to be 
                    used. If no dataset is given, an attempt is made to identify 
                    the dataset based on the current working directory""",
                    constraints=EnsureDataset() | EnsureNone()),
            path=Parameter(
                    args=("path",),
                    metavar="PATH",
                    nargs="+",
                    doc="""path to DICOM files""",
                    constraints=EnsureStr() | EnsureNone()),
            spec=Parameter(
                    args=("-s", "--spec",),
                    metavar="SPEC",
                    doc="""file to store the specification in""",
                    constraints=EnsureStr() | EnsureNone()),
            subject=Parameter(
                    args=("--subject",),
                    metavar="SUBJECT",
                    doc="""subject identifier. If not specified, an attempt will be made 
                        to derive SUBJECT from DICOM headers""",
                    constraints=EnsureStr() | EnsureNone()),
            anon_subject=Parameter(
                    args=("--anon-subject",),
                    metavar="ANON_SUBJECT",
                    doc="""TODO""",
                    constraints=EnsureStr() | EnsureNone()),
            session=Parameter(
                    args=("--session",),
                    metavar="SESSION",
                    doc="""session identifier. If not specified, an attempt will be made 
                    to derive SESSION from DICOM headers""",
                    constraints=EnsureStr() | EnsureNone()),
            properties=Parameter(
                    args=("--properties",),
                    metavar="PATH or JSON string",
                    doc="""""",
                    constraints=EnsureStr() | EnsureNone()),

            recursive=recursion_flag,
            # TODO: invalid, since datalad-metadata doesn't support it:
            # recursion_limit=recursion_limit,
    )

    @staticmethod
    @datasetmethod(name='hirni_dicom2spec')
    @eval_results
    def __call__(path=None, spec=None, dataset=None, subject=None,
                 anon_subject=None, session=None, recursive=False,
                 properties=None):

        dataset = require_dataset(dataset, check_installed=True,
                                  purpose="spec from dicoms")

        from datalad.utils import assure_list
        if path is not None:
            path = assure_list(path)
            path = [resolve_path(p, dataset) for p in path]
        else:
            raise InsufficientArgumentsError(
                "insufficient arguments for dicom2spec: a path is required")

        # TODO: We should be able to deal with several paths at once
        #       ATM we aren't (see also commit + message of actual spec)
        assert len(path) == 1

        if not spec:
            raise InsufficientArgumentsError(
                "insufficient arguments for dicom2spec: a spec file is required")
        else:
            spec = resolve_path(spec, dataset)

        spec_series_list = \
            [r for r in json_py.load_stream(spec)] if op.exists(spec) else list()

        # get dataset level metadata:
        found_some = False
        for meta in metadata(
                path,
                dataset=dataset,
                recursive=recursive,
                reporton='datasets',
                return_type='generator',
                result_renderer='disabled'):
            if meta.get('status', None) not in ['ok', 'notneeded']:
                yield meta
                continue

            if 'dicom' not in meta['metadata']:

                # TODO: Really "notneeded" or simply not a result at all?
                yield dict(
                        status='notneeded',
                        message=("found no DICOM metadata for %s",
                                 meta['path']),
                        path=meta['path'],
                        type='dataset',
                        action='dicom2spec',
                        logger=lgr)
                continue

            if 'Series' not in meta['metadata']['dicom'] or \
                    not meta['metadata']['dicom']['Series']:
                yield dict(
                        status='impossible',
                        message=("no image series detected in DICOM metadata of"
                                 " %s", meta['path']),
                        path=meta['path'],
                        type='dataset',
                        action='dicom2spec',
                        logger=lgr)
                continue

            found_some = True

            overrides = dict()
            if properties:
                # load from file or json string
                props = json_py.load(properties) \
                        if op.exists(properties) else json_py.loads(properties)
                # turn into editable, pre-approved records
                props = {k: dict(value=v, approved=True) for k, v in props.items()}
                overrides.update(props)

            spec_series_list = add_to_spec(meta,
                                           spec_series_list,
                                           op.dirname(spec),
                                           subject=subject,
                                           anon_subject=anon_subject,
                                           # session=session,
                                           # TODO: parameter "session" was what
                                           # we now call acquisition. This is
                                           # NOT a good default for bids_session!
                                           # Particularly wrt to anonymization
                                           overrides=overrides
                                           )

        if not found_some:
            yield dict(status='impossible',
                       message="found no DICOM metadata",
                       # TODO: What to return here in terms of "path" and "type"?
                       path=path,
                       type='file',
                       action='dicom2spec',
                       logger=lgr)
            return

        lgr.debug("Storing specification (%s)", spec)
        # store as a stream (one record per file) to be able to
        # easily concat files without having to parse them, or
        # process them line by line without having to fully parse them
        from ..support.helpers import sort_spec
        spec_series_list = sorted(spec_series_list, key=lambda x: sort_spec(x))
        json_py.dump2stream(spec_series_list, spec)

        from datalad.distribution.add import Add

        for r in Add.__call__(spec,
                              to_git=True,
                              save=True,
                              message="[HIRNI] Added study specification "
                                      "snippet for %s" %
                                      op.relpath(path[0], dataset.path),
                              return_type='generator',
                              result_renderer='disabled'):
            if r.get('status', None) not in ['ok', 'notneeded']:
                yield r
            elif r['path'] == spec and r['type'] == 'file':
                r['action'] = 'dicom2spec'
                r['logger'] = lgr
                yield r
            elif r['type'] == 'dataset':
                # 'ok' or 'notneeded' for a dataset is okay, since we commit
                # the spec. But it's not a result to yield
                continue
            else:
                # anything else shouldn't happen
                yield dict(status='error',
                           message=("unexpected result from Add: %s", r),
                           path=spec,
                           type='file',
                           action='dicom2spec',
                           logger=lgr)
