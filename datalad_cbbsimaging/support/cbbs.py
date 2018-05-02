import logging

lgr = logging.getLogger(__name__)


def create_key(template, outtype=('nii.gz',), annotation_classes=None):
    if template is None or not template:
        raise ValueError('Template must be a valid format string')

    return template, outtype, annotation_classes


class SpecLoader(object):
    """
    Persistent object to hold the study specification and not read the JSON on
    each invocation of `infotodict`. Module level attribute for the spec itself
    doesn't work, since the env variable isn't necessarily available at first
    import.
    """

    def __init__(self):
        self._spec = None

    def get_study_spec(self):
        if self._spec is None:
            from os import environ
            import datalad.support.json_py
            filename = environ.get('CBBS_STUDY_SPEC')
            if filename:
                self._spec = [d for d in
                              datalad.support.json_py.load_stream(filename)]
            else:
                # TODO: Just raise or try a default location first?
                raise ValueError("No study specification provided. "
                                 "Set environment variable CBBS_STUDY_SPEC "
                                 "to do so.")
        return self._spec


_spec = SpecLoader()


def validate_spec(spec):

    if not spec:
        raise ValueError("Image series specification is empty.")

    # check converter
    if spec['converter']['value'] == 'ignore':
        lgr.debug("Skip series %s (marked 'ignore' in spec)", spec['uid'])
        return False

    if spec['converter']['value'] != 'heudiconv':
        lgr.debug("Skip series %s since it's not supposed to be converted by "
                  "heudiconv.", spec['uid'])
        return False

    # mandatory keys for any spec dict (not only dicomseries)
    for k in spec.keys():
        # automatically managed keys with no subdict:
        # TODO: Where to define this list?
        # TODO: Test whether those are actually present!
        if k in ['type', 'status', 'location', 'uid', 'dataset_id',
                 'dataset_refcommit']:
            continue
        if not spec[k]['value']:
            lgr.warning("DICOM series specification (UID: {uid}) has no value "
                        "for key '{key}'.".format(uid=spec['uid'], key=k))

    if spec['type'] != 'dicomseries':
        raise ValueError("Specification not of type 'dicomseries'.")

    if 'uid' not in spec.keys() or not spec['uid']:
        raise ValueError("Invalid image series UID.")

    # subject
    if 'subject' not in spec.keys() or not spec['subject']['value']:
        raise ValueError("Found no subject in specification for series %s." %
                         spec['uid'])

    # data type
    if 'data_type' not in spec.keys() or not spec['subject']['value']:
        raise ValueError("Found no data type in specification for series %s." %
                         spec['uid'])

    return True


# TODO: can be removed, whenever nipy/heudiconv #197 is solved
def infotoids(seqinfos, outdir):
    return {'locator': None,
            'session': None,
            'subject': None}


def infotodict(seqinfo):
    """Heuristic evaluator for determining which runs belong where

    allowed template fields - follow python string module:

    item: index within category
    subject: participant id
    seqitem: run number during scanning
    subindex: sub index within group
    """

    info = dict()
    for idx, s in enumerate(seqinfo):

        # find in spec:
        candidates = [series for series in _spec.get_study_spec()
                      if str(s.uid) == series['uid']]
        if not candidates:
            raise ValueError("Found no match for seqinfo: %s" % str(s))
        if len(candidates) != 1:
            raise ValueError("Found %s match(es) for series UID %s" %
                             (len(candidates), s.uid))
        series_spec = candidates[0]

        if not validate_spec(series_spec):
            lgr.debug("Series invalid (%s). Skip.", str(s.uid))
            continue

        dirname = filename = "sub-{}".format(series_spec['subject']['value'])
        # session
        if series_spec['session'] and series_spec['session']['value']:
            dirname += "/ses-{}".format(series_spec['session']['value'])
            filename += "_ses-{}".format(series_spec['session']['value'])

        # data type
        data_type = series_spec['data_type']['value']

        dirname += "/{}".format(data_type)
        if data_type == 'func':
            # func/sub-<participant_label>[_ses-<session_label>]
            # _task-<task_label>[_acq-<label>][_rec-<label>][_run-<index>][_echo-<index>]_bold.nii[.gz]
            if series_spec['task']['value']:
                filename += "_task-{}".format(series_spec['task']['value'])

            # TODO: [_acq-<label>][_rec-<label>]

            if series_spec['run']['value']:
                filename += "_run-{}".format(series_spec['run']['value'])

            filename += "_bold"

        if data_type == 'anat':
            # anat/sub-<participant_label>[_ses-<session_label>]
            # [_acq-<label>][_ce-<label>][_rec-<label>][_run-<index>][_mod-<label>]_<modality_label>.nii[.gz]

            # TODO: [_acq-<label>][_ce-<label>][_rec-<label>]

            if series_spec['run']['value']:
                filename += "_run-{}".format(series_spec['run']['value'])

            # TODO: [_mod-<label>]

            if series_spec['modality'] and series_spec['modality']['value']:
                filename += "_{}".format(series_spec['modality']['value'])

        # TODO: data_type: dwi, fmap

        key = create_key(dirname + '/' + filename)
        if key not in info:
            info[key] = []

        info[key].append(s[2])

    return info
