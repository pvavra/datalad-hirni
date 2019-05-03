"""dicom2spec default rules"""


# TODO: RF: Repronim rules to dedicated rule set. Plus: default numbering for runs, aborted run detection etc.
# Are default rules a fallback or a configured rule set?

from datalad_hirni.support.BIDS_helper import apply_bids_label_restrictions


def _guess_subject(record):
    # Subject identification depends on scanner site:
    # Note: This possibly is overspecified ATM. Let's check out all
    #       scanners before being clear about how to safely distinguish
    #       them.
    if record.get("StationName") == "3T-PHILIPSMR" and \
            record.get(
                    "InstitutionName") == "Leibniz Institut Magdeburg" and \
            record.get("Manufacturer") == "Philips Medical Systems" and \
            record.get("ManufacturerModelName") == "Achieva dStream":

        subject = record.get("PatientName", None)
    elif record.get("StationName") == "AWP66017" and \
            record.get("InstitutionName") == "Neurologie" and \
            record.get("Manufacturer") == "SIEMENS" and \
            record.get("ManufacturerModelName") == "Prisma":

        subject = record.get("PatientID", None)
        if subject:
            subject = subject.split("_")[0]
    elif record.get("StationName") == "PCR7T1-15" and \
            record.get("InstitutionName") == "LIN" and \
            record.get("Manufacturer") == "SIEMENS" and \
            record.get("ManufacturerModelName") == "Investigational_Device_7T":

        subject = record.get("PatientID", None)
        if subject:
            subject = subject.split("_")[0]
    else:
        subject = record.get("PatientID", None)

    return subject


def _guess_task(record):

    protocol = record.get("ProtocolName", None)
    if protocol:
        import re
        prot_parts = re.split('_|-|\s', protocol.lower())
        try:
            idx = prot_parts.index("task")
            task = prot_parts[idx + 1]
            return task
        except (ValueError, IndexError):
            # default to entire protocol name?
            # This should actually check the results of other guesses
            # (like modality) to determine a better default than nothing.
            # At least parts of the protocol name that were already matched elsewhere
            # should be excluded
            return None
    else:
        # default to entire protocol name?
        # see above
        return None


def _guess_modality(record):

    protocol = record.get("ProtocolName", None)
    if protocol:

        # BEGIN Additional rule for forrest-structural
        # TODO: Probably to be moved to some rule enhancement
        if "VEN_BOLD" in protocol:
            # TODO: Not clear yet; swi might be considered a datatype rather than
            # a modality by respective BIDS extension:
            # https://docs.google.com/document/d/1kyw9mGgacNqeMbp4xZet3RnDhcMmf4_BmRgKaOkO2Sc
            return "swi"

        if "DTI_" in protocol:
            # TODO: What actually is the relevant part of protocol here?
            return "dwi"

        if "field map" in protocol:
            return "fieldmap"
        # END

        import re
        prot_parts = re.split('_|-|\s', protocol.lower())
        direct_search_terms = ["t1", "t1w", "t2", "t2w",
                               "t1rho", "t1map", "t2map", "t2star", "flair",
                               "flash", "pd", "pdmap", "pdt2", "inplanet1",
                               "inplanet2", "angio", "dwi", "phasediff",
                               "phase1", "phase2", "magnitude1", "magnitude2",
                               "fieldmap", "epi", "meg", "bold"]

        for m in direct_search_terms:
            if m in prot_parts:
                return m

        # BEGIN: Additional rule for forrest-structural
        # TODO: Probably to be moved to some rule enhancement
        if "st1w" in prot_parts:
            return "t1w"
        if "st2w" in prot_parts:
            return "t2w"
        if "tof" in prot_parts:
            return "angio"
        # END

        # TEMP: Reproin workaround
        if "func" in prot_parts:
            return "bold"

    # found nothing, but modality isn't necessarily required
    return None


def _guess_run(record):
    protocol = record.get("ProtocolName", None)
    if protocol:
        import re
        prot_parts = re.split('_|-|\s', protocol.lower())
        try:
            idx = prot_parts.index("run")
            run = prot_parts[idx + 1]
            # TODO: Actually check number of runs and do the zero padding
            # accordingly (prob. still minimum 2 digits)
            # Q&D:
            if len(run) == 1:
                run = "0" + run
            return run
        except (ValueError, IndexError):
            # no result yet
            pass

        pattern = re.compile(r'r[0-9]+')
        for part in prot_parts:
            match = re.match(pattern, part)
            if match:
                run = match.group(0)[1:]
                # TODO: correct padding; see above
                if len(run) == 1:
                    run = "0" + run
                return run
    # default to None will lead to counting series with same protocol
    return None


def _guess_session(record):

    protocol = record.get("ProtocolName", None)
    if protocol:
        import re
        match = re.search(r"(?<=ses[_-])([a-zA-Z0-9]+).*", protocol)
        if match:
            return match.group(1)
        else:
            return None
    else:
        return None

# MPRAGE => T1w


# Philips 3T:
# SeriesDescription
#

# SmartBrain_ AHAScout => localizer

# field map/fieldmap  & _check   > fmap


# Studydescription: TASK_skdjfdsnfs


class DefaultRules(object):

    def __init__(self, dicommetadata):
        """

        Parameter
        ----------
        dicommetadata: list of dict
            dicom metadata as extracted by datalad; one dict per image series
        """
        self._dicom_series = dicommetadata
        self.runs = dict()

    def __call__(self, subject=None, anon_subject=None, session=None):
        """

        Parameters
        ----------

        Returns
        -------
        list of tuple (dict, bool)
        """
        spec_dicts = []
        for dicom_dict in self._dicom_series:
            spec_dicts.append((self._rules(dicom_dict,
                                           subject=subject,
                                           anon_subject=anon_subject,
                                           session=session),
                               self.series_is_valid(dicom_dict)
                               )
                              )
        return spec_dicts

    def _rules(self, series_dict, subject=None, anon_subject=None,
               session=None):

        protocol_name = series_dict.get('ProtocolName', None)

        run = _guess_run(series_dict)
        # TODO: Default numbering should still fill up leading zero(s)
        if run is None:
            # count appearances of protocol as a guess:
            if protocol_name in self.runs:
                self.runs[protocol_name] += 1
            else:
                self.runs[protocol_name] = 1

        # TODO: Decide how to RF to apply custom rules. To be done within
        # datalad-neuroimaging, then just a custom one here.

        return {
                # Additional (humanreadable) identification:
                # SeriesNumber
                # SeriesDate
                # SeriesTime
                'description': series_dict['SeriesDescription'] if "SeriesDescription" in series_dict else '',
                'comment': '',
                'subject': apply_bids_label_restrictions(_guess_subject(series_dict) if not subject else subject),
                'anon-subject': apply_bids_label_restrictions(anon_subject) if anon_subject else None,
                'bids-session': apply_bids_label_restrictions(_guess_session(series_dict) if not session else session),
                'bids-task': apply_bids_label_restrictions(_guess_task(series_dict)),
                'bids-run': apply_bids_label_restrictions(run) if run else str(self.runs[protocol_name]),
                'bids-modality': apply_bids_label_restrictions(_guess_modality(series_dict)),

                # TODO: No defaults yet (May be there shouldn't be defaults, but
                # right now, that's not a conscious decision ...):
                'bids-acquisition': apply_bids_label_restrictions(None), #acq
                'bids-contrast-enhancement': apply_bids_label_restrictions(None), # ce
                'bids-reconstruction-algorithm': apply_bids_label_restrictions(None), #rec
                'bids-echo': apply_bids_label_restrictions(None), #echo
                'bids-direction': apply_bids_label_restrictions(None), #dir

                'id': series_dict.get('SeriesNumber', None),
                }

    def series_is_valid(self, series_dict):
        # filter "Series" entries from dataset metadata here, in order to get rid of
        # things, that aren't relevant image series
        # Those series are supposed to be ignored during conversion.
        # TODO: RF: integrate with rules definition

        # Note:
        # In 3T_visloc, SeriesNumber 0 is associated with ProtocolNames
        # 'DEFAULT PRESENTATION STATE' and 'ExamCard'.
        # All other SeriesNumbers have 1:1 relation to ProtocolNames and have 3-4
        # digits.
        # In 7T_ad there is no SeriesNumber 0 and the SeriesNumber doesn't have a 1:1
        # relation to ProtocolNames
        # Note: There also is a SeriesNumber 99 with protocol name 'Phoenix Document'?

        # Philips 3T Achieva
        if series_dict['SeriesNumber'] == 0 and \
                series_dict['ProtocolName'] in ['DEFAULT PRESENTATION STATE',
                                                'ExamCard']:
            return False
        return True


__datalad_hirni_rules = DefaultRules
