# emacs: -*- mode: python-mode; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# -*- coding: utf-8 -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Test dicom2spec command; DICOM metadata based specification creation"""

import os.path as op

from datalad.api import Dataset

from datalad.tests.utils import (
    assert_result_count,
    ok_clean_git,
    with_tempfile,
    eq_
)

from datalad.utils import get_tempfile_kwargs

from datalad_neuroimaging.tests.utils import (
    get_dicom_dataset,
    create_dicom_tarball
)


# TODO:
#
# - invalid calls
# - pass properties
# - test default rules
# - custom vs. configured specfile
# - test results
# - spec file in git? => should stay in git

# - build study ds only once and then clone it for test, since we only need metadata?

# def _setup_study_dataset():
#     """helper to build study dataset only once
#
#     Note, that dicom2spec relies on DICOM metadata only!
#     """
#
#     import tempfile
#     kwargs = get_tempfile_kwargs()
#     path = tempfile.mkdtemp(**kwargs)
#     f_dicoms = get_dicom_dataset('functional')
#     s_dicoms = get_dicom_dataset('structural')
#     ds = Dataset.create(path)
#     ds.run_procedure('setup_hirni_dataset')
#     ds.install(source=f_dicoms, path='acq_func')
#     ds.install(source=s_dicoms, path='acq_struct')
#     ds.aggregate_metadata(recursive=True, update_mode='all')
#
#     # TODO: Figure how to add it to things to be removed after tests ran
#     return ds.path

# studyds_path = _setup_study_dataset()


@with_tempfile
def test_dicom2spec(path):

    # ###  SETUP ###
    dicoms = get_dicom_dataset('structural')

    ds = Dataset.create(path)
    ds.run_procedure('setup_hirni_dataset')
    ds.install(source=dicoms, path='acq100')
    ds.aggregate_metadata(recursive=True, update_mode='all')
    # ### END SETUP ###

    # TODO: should it be specfile or acq/specfile? => At least doc needed,
    # if not change
    res = ds.hirni_dicom2spec(path='acq100', spec='spec_structural.json')

    # check for actual location of spec_structural!
    # => studyds root!

    assert_result_count(res, 2)
    assert_result_count(res, 1, path=op.join(ds.path, 'spec_structural.json'))
    assert_result_count(res, 1, path=op.join(ds.path, '.gitattributes'))
    ok_clean_git(ds.path)
