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

from datalad.api import (
    Dataset,
    rev_create
)

from datalad.tests.utils import (
    assert_result_count,
    assert_not_in,
    ok_clean_git,
    with_tempfile,
    eq_
)

from datalad.utils import get_tempfile_kwargs

from datalad_neuroimaging.tests.utils import (
    get_dicom_dataset,
    create_dicom_tarball
)

from datalad.support.json_py import load_stream
import datalad_hirni.support.hirni_heuristic as heuristic

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
def test_default_rules(path):

    f_dicoms = get_dicom_dataset('functional')
    s_dicoms = get_dicom_dataset('structural')

    ds = rev_create(path)
    ds.install(source=f_dicoms, path=op.join("func_acq", "dicoms"))
    ds.install(source=s_dicoms, path=op.join("struct_acq", "dicoms"))

    ds.aggregate_metadata(recursive=True, update_mode='all')

    # TODO: spec path should prob. relate to `path`!
    ds.hirni_dicom2spec(path=op.join("func_acq", "dicoms"), spec=op.join("func_acq", "studyspec.json"))
    ds.hirni_dicom2spec(path=op.join("struct_acq", "dicoms"), spec=op.join("struct_acq", "studyspec.json"))

    for snippet in load_stream(op.join(path, "func_acq", "studyspec.json")):

        # type
        assert "type" in snippet.keys()
        assert snippet["type"] in ["dicomseries", "dicomseries:all"]

        # no comment in default spec
        assert not heuristic.has_specval(snippet, 'comment') or not heuristic.get_specval(snippet, 'comment')
        # description
        assert heuristic.has_specval(snippet, 'description')
        eq_(heuristic.get_specval(snippet, 'description'), "func_task-oneback_run-1")
        # subject
        assert heuristic.has_specval(snippet, 'subject')
        eq_(heuristic.get_specval(snippet, 'subject'), '02')
        # modality
        assert heuristic.has_specval(snippet, 'bids-modality')
        eq_(heuristic.get_specval(snippet, 'bids-modality'), 'bold')
        # task
        assert heuristic.has_specval(snippet, "bids-task")
        eq_(heuristic.get_specval(snippet, "bids-task"), "oneback")
        # run
        assert heuristic.has_specval(snippet, "bids-run")
        eq_(heuristic.get_specval(snippet, "bids-run"), "01")
        # id
        assert heuristic.has_specval(snippet, "id")
        eq_(heuristic.get_specval(snippet, "id"), 401)

    for snippet in load_stream(op.join(path, "struct_acq", "studyspec.json")):

        # type
        assert "type" in snippet.keys()
        assert snippet["type"] in ["dicomseries", "dicomseries:all"]
        # no comment in default spec
        assert not heuristic.has_specval(snippet, 'comment') or not heuristic.get_specval(snippet, 'comment')
        # description
        assert heuristic.has_specval(snippet, 'description')
        eq_(heuristic.get_specval(snippet, 'description'), "anat-T1w")
        # subject
        assert heuristic.has_specval(snippet, 'subject')
        eq_(heuristic.get_specval(snippet, 'subject'), '02')
        # modality
        assert heuristic.has_specval(snippet, 'bids-modality')
        eq_(heuristic.get_specval(snippet, 'bids-modality'), 't1w')
        # run
        assert heuristic.has_specval(snippet, "bids-run")
        eq_(heuristic.get_specval(snippet, "bids-run"), "1")  # TODO: Default numbering should still fill up leading zero(s)


@with_tempfile
def test_custom_rules(path):

    dicoms = get_dicom_dataset('structural')
    ds = rev_create(path)
    ds.install(source=dicoms, path="acq")
    ds.aggregate_metadata(recursive=True, update_mode='all')
    ds.hirni_dicom2spec(path="acq", spec="studyspec.json")

    # assertions wrt spec

    # TODO: This is wrong, I think. Path should be acq/studyspec.json instead.
    for spec_snippet in load_stream(op.join(path, 'studyspec.json')):

        # no comment in default spec
        assert not heuristic.has_specval(spec_snippet, 'comment') or not heuristic.get_specval(spec_snippet, 'comment')
        # subject
        assert heuristic.has_specval(spec_snippet, 'subject')
        eq_(heuristic.get_specval(spec_snippet, 'subject'), '02')
        # modality
        assert heuristic.has_specval(spec_snippet, 'bids-modality')
        eq_(heuristic.get_specval(spec_snippet, 'bids-modality'), 't1w')

    # set config to use custom rules
    import datalad_hirni
    ds.config.add("datalad.hirni.dicom2spec.rules",
                  op.join(op.dirname(datalad_hirni.__file__),
                          'resources',
                          'rules',
                          'test_rules.py'),
                  )

    # do again with configured rules
    import os
    os.unlink(op.join(path, 'studyspec.json'))

    ds.hirni_dicom2spec(path="acq", spec="studyspec.json")

    # assertions wrt spec

    # TODO: This is wrong, I think. Path should be acq/studyspec.json instead.
    for spec_snippet in load_stream(op.join(path, 'studyspec.json')):

        # now there's a comment in spec
        assert heuristic.has_specval(spec_snippet, 'comment')
        eq_(heuristic.get_specval(spec_snippet, 'comment'), "These rules are for unit testing only")


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
