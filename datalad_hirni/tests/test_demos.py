# emacs: -*- mode: python-mode; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# -*- coding: utf-8 -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Test demos from documentation"""


import os.path as op
from datalad.api import (
    Dataset,
    drop
)
from datalad.tests.utils import (
    assert_result_count,
    assert_in,
    ok_clean_git,
    with_tempfile,
    assert_equal,
    usecase,
    ok_file_under_git,
    assert_repo_status,
    assert_true
)


@usecase
@with_tempfile
def test_demo_raw_ds(path):

    ds = Dataset(path)
    ds.create()  # TODO: May be move to ds.create(cfg_proc='hirni') in demo
    ds.run_procedure('cfg_hirni')

    # clean repo with an annex:
    assert_repo_status(ds.repo, annex=True)

    # README, dataset_description.json and studyspec.json at toplevel and in git
    for f in ['README', 'studyspec.json', 'dataset_description.json']:
        ok_file_under_git(ds.path, f, annexed=False)

    # toolbox installed under code/hirni-toolbox
    subs = ds.subdatasets()
    assert_result_count(subs, 1)
    assert_result_count(subs, 1, path=op.join(ds.path, 'code', 'hirni-toolbox'))

    ds.hirni_import_dcm('https://github.com/datalad/example-dicom-structural/archive/master.tar.gz',
                        'acq1',
                        anon_subject='001')

    # acquisition directory and studyspec created + subdataset 'dicoms' within the acquisition dir
    for f in [op.join(ds.path, 'acq1'),
              op.join(ds.path, 'acq1', 'studyspec.json'),
              op.join(ds.path, 'acq1', 'dicoms')
              ]:
        assert_true(op.exists(f))
    subs = ds.subdatasets()
    assert_result_count(subs, 2)
    assert_result_count(subs, 1, path=op.join(ds.path, 'code', 'hirni-toolbox'))
    assert_result_count(subs, 1, path=op.join(ds.path, 'acq1', 'dicoms'))

    # TODO: check actual spec? (Prob. sufficient to test for that in dedicated import-dcm/dcm2spec tests
    # TODO: check dicom metadata

    ds.hirni_import_dcm('https://github.com/datalad/example-dicom-functional/archive/master.tar.gz',
                        'acq2',
                        anon_subject='001')

    # acquisition directory and studyspec created + subdataset 'dicoms' within the acquisition dir
    for f in [op.join(ds.path, 'acq2'),
              op.join(ds.path, 'acq2', 'studyspec.json'),
              op.join(ds.path, 'acq2', 'dicoms')
              ]:
        assert_true(op.exists(f))
    subs = ds.subdatasets()
    assert_result_count(subs, 3)
    assert_result_count(subs, 1, path=op.join(ds.path, 'code', 'hirni-toolbox'))
    assert_result_count(subs, 1, path=op.join(ds.path, 'acq1', 'dicoms'))
    assert_result_count(subs, 1, path=op.join(ds.path, 'acq2', 'dicoms'))

    # Note from demo: The calls to `git annex addurl` and `datalad rev-save` currently replace a single call to
    # `datalad download-url` due to a bug in that command.
    events_file = op.join('acq2', 'events.tsv')
    ds.repo.add_url_to_file(file_=events_file,
                            url='https://github.com/datalad/example-dicom-functional/raw/master/events.tsv')
    ds.rev_save(message="Added stimulation protocol for acquisition 2")

    ok_file_under_git(ds.path, events_file, annexed=True)

    ds.hirni_spec4anything(events_file,
                           properties='{"procedures": {"procedure-name": "copy-converter", "procedure-call": "bash {script} {{location}} {ds}/sub-{{bids-subject}}/func/sub-{{bids-subject}}_task-{{bids-task}}_run-{{bids-run}}_events.tsv"}, "type": "events_file"}')

    ok_file_under_git(ds.path, op.join('acq2', 'studyspec.json'), annexed=False)
    assert_repo_status(ds.repo, annex=True)

    # TODO: check spec!
    # compare to:
    # % datalad install -s https://github.com/psychoinformatics-de/hirni-demo --recursive
