"""Template for writing custom rules for dicom2spec"""


class MyDICOM2SpecRules(object):

    def __init__(self, dicommetadata):
        """

        Parameter
        ----------
        dicommetadata: list of dict
            dicom metadata as extracted by datalad; one dict per image series
        """
        self._dicom_series = dicommetadata

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

        return {'description': series_dict['SeriesDescription']
                if "SeriesDescription" in series_dict else '',

                'comment': 'I actually have no clue',
                'subject': series_dict['PatientID'] if not subject else subject,
                'anon-subject': anon_subject if anon_subject else None,
                'bids-session': session if session else None
                }

    def series_is_valid(self, series_dict):

        return series_dict['ProtocolName'] != 'ExamCard'


__datalad_hirni_rules = MyDICOM2SpecRules
