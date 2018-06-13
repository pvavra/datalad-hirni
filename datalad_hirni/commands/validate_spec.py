"""Validate a study specification"""


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
from datalad.support.json_py import load_stream
from datalad.interface.annotate_paths import AnnotatePaths
from datalad.interface.results import get_status_dict

import logging
lgr = logging.getLogger('datalad.hirni.validate_spec')


class SnippetValidator(object):

    known_spec_types = [
        'dicomseries',
        'generic_file',
        'generic_directory',
        'generic_dataset',
    ]

    required_keys = [
        'type',
        'location',


        # ##########
        # Issue: what about other snippets, not created via metadata
        #        extraction? Currently spec4anything would ask the dataset
        #        for its refcommit via datalad-metadata. But since there might
        #        be no datalad-metadata relevant commit, that refcommit would
        #        be null.

        'dataset_id',
        'dataset_refcommit',
        # ###############

        'converter'
    ]

    optional_keys = ['description', 'comment', 'status']

    def __call__(self, spec, path):
        if isinstance(spec, dict):
            for r in self.validate_snippet(spec, path):
                yield r
        elif isinstance(spec, list):
            for r in self.validate_consistency(spec):
                yield r

    # TODO: 'path' option isn't really nice
    def validate_snippet(self, snippet, path):
        """
        Parameters
        ----------
        snippet: dict
        path: PathRI
            path to spec file relative to dataset root
        """

        if not snippet:
            yield {'status': 'error',
                   'message': "empty specification snippet"}
            return

        # TODO: How to better identify which snippet we are talking
        # about when reporting errors?
        # Prob. type specific solution via validator classes per snippet
        # type

        # check mandatory entries:
        for k in self.required_keys:
            if k not in snippet:
                yield {'status': 'error',
                       'message': "snippet missing key '%s'" % k}
                continue
            if not snippet[k] or \
                (isinstance(snippet[k], dict) and not snippet[k]['value']):
                yield {'status': 'error',
                       'message': "empty entry for '%s'" % k}

        # check optional keys:
        for k in self.optional_keys:
            if k in snippet:
                if not snippet[k] or \
                        (isinstance(snippet[k], dict) and
                         not snippet[k]['value']):
                    lgr.warning("empty entry for '%s'" % k)

        # check whether we know all keys:
        for k in snippet:
            if k not in self.required_keys + self.optional_keys:
                yield {'status': 'error',
                       'message': "unknown key '%s'" % k}

        if snippet['type'] not in self.known_spec_types:
            yield {'status': 'error',
                   'message': "unknown specification type '%s'" % snippet['type']}

        # 'location' is expected to point into spec file's tree:
        prefix = posixpath.commonprefix([path.posixpath,
                                         snippet['location']])
        if not prefix == posixpath.dirname(path.posixpath) + '/':
            yield {'status': 'error',
                   'message': "'location' outside specification's tree"}

        # TODO: not approved values => warning
        # TODO: check 'dataset_id', 'dataset_refcommit'? Requires the dataset!

    # TODO: There might be non-acquisition specs.
    #       This would need an option here.
    def validate_consistency(self, spec):
        yield


class DicomValidator(SnippetValidator):

    required_keys = SnippetValidator.required_keys + \
                    ['uid', 'id', 'subject', 'converter']

    # TODO: What's optional here? Check for valid combinations?
    bids_keys = [
                'bids_session',
                'bids_task',
                'bids_run',
                'bids_modality',
                'bids_acquisition',
                'bids_contrast_enhancement',
                'bids_reconstruction_algorithm',
                'bids_echo',
                'bids_direction'
    ]

    optional_keys = SnippetValidator.optional_keys + ['anon_subject'] + bids_keys

    def validate_snippet(self, snippet, path):
        for r in super(DicomValidator, self).validate_snippet(snippet, path):
            yield r

    def validate_consistency(self, spec):
        for r in super(DicomValidator, self).validate_consistency(spec):
            yield r

        # Notes: - skip everything non-dicom
        #        - assume one acquisition in spec
        #        - check IDs (ambiguous?)
        #        - same subject?
        #        - any contradiction in sessions, tasks, etc?


type_validator_map = {'dicomseries': DicomValidator,
                      'generic_file': SnippetValidator,
                      'generic_directory': SnippetValidator,
                      'generic_dataset': SnippetValidator,
                      }


@build_doc
class ValidateSpec(Interface):
    """
    """

    _params_ = dict(
        dataset=Parameter(
            args=("-d", "--dataset"),
            metavar='PATH',
            doc="""specify the study dataset. If no dataset is given, an attempt is 
            made to identify the dataset based on the current working directory 
            and/or the `path` given""",
            constraints=EnsureDataset() | EnsureNone()),
        path=Parameter(
            args=("path",),
            metavar='PATH',
            doc="""path(s) of the specification file(s) to validate.""",
            nargs="*",
            constraints=EnsureStr()),
    )

    @staticmethod
    @datasetmethod(name='hirni_validate_spec')
    @eval_results
    def __call__(path, dataset=None):

        res_kwargs = dict(action='hirni validate spec', logger=lgr)

        for ap in AnnotatePaths.__call__(
                dataset=Interface.get_refds_path(dataset) ,
                path=path,
                action='hirni validate spec',
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

            res_kwargs['path'] = ap['path']
            res_kwargs['type'] = ap['type']

            if not ap.get('type', None) == 'file':
                yield get_status_dict(status='impossible',
                                      message="not a file",
                                      **res_kwargs)
                continue

            # ### This might become superfluous. See datalad-gh-2653
            ap_path = PathRI(ap['path'])
            ds_path = PathRI(ap['parentds'])
            # ###

            # find acquisition and respective specification file:
            rel_path = posixpath.relpath(ap_path.posixpath, ds_path.posixpath)

            # TODO: This needs more generalization as we want to have higher
            # level specification snippets, that aren't within an acquisition
            path_parts = rel_path.split('/')
            if len(path_parts) < 2:
                yield get_status_dict(status='error',
                                      message="not within an acquisition",
                                      **res_kwargs)
                continue
            acq = path_parts[0]

            # load the spec
            spec_ok = True
            spec = []
            for snippet in load_stream(ap_path.posixpath):
                if not snippet:
                    spec_ok = False
                    yield get_status_dict(status='error',
                                          message="empty specification snippet",
                                          **res_kwargs)
                    continue

                if 'type' not in snippet or not snippet['type']:
                    # although this is already invalidating the snippet,
                    # let's pass that onto default SnippetValidator in order to
                    # get more results on what's wrong with that snippet
                    validator = SnippetValidator()
                else:
                    validator = type_validator_map[snippet['type']]()

                for r in validator(snippet, PathRI(rel_path)):
                    if r['status'] == 'error':
                        spec_ok = False
                    r.update(res_kwargs)
                    yield r

                # Note: for consistency checks we need all of them at once
                spec.append(snippet)

            # TODO consistency checks:

            if spec_ok:
                yield get_status_dict(status='ok',
                                      message="specification valid",
                                      **res_kwargs)

