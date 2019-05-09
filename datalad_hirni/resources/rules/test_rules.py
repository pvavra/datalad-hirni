"""Dummy rules for dicom2spec. For use with tests only."""


class DICOM2SpecTestRules1(object):

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


        # TODO: distinguish plain rules application vs append?

        return {'comment': 'Rules1: These rules are for unit testing only',
                }

    def series_is_valid(self, series_dict):

        return True


__datalad_hirni_rules = DICOM2SpecTestRules1

