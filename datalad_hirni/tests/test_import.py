from os.path import join as opj, exists

import datalad_hirni
from datalad.api import Dataset
from datalad.api import hirni_create_study

from datalad.tests.utils import assert_result_count
from datalad.tests.utils import ok_clean_git, ok_exists, ok_file_under_git
from datalad.tests.utils import with_tempfile, known_failure_direct_mode

from datalad_neuroimaging.tests.utils import get_dicom_dataset, create_dicom_tarball


# Note: In direct mode the entire three branch approach on importing seems to
# have an issue. The tarball is a broken in symlink in 'incoming-processed',
# before add_archive_content is even called. On the other hand annex-get works
# for it, so it's not like nothing worked at all.
# May be have to somehow account for direct mode when switching branches.
# TODO: Narrow down what's broken and make it an issue
@known_failure_direct_mode
@with_tempfile(mkdir=True)
@with_tempfile
def test_import_tarball(src, ds_path):

    filename = opj(src, "structural.tar.gz")
    ds = get_dicom_dataset(flavor="structural")
    from datalad.api import export_archive
    ds.export_archive(filename, archivetype="tar", compression="gz",
                      missing_content="ignore")

    ds = hirni_create_study(ds_path)

    # adapt import layout rules for example ds, since hirni default
    # doesn't apply:
    ds.config.set("datalad.hirni.import.session-format",
                  "sub-{PatientID}", where='dataset')

    # import into a session defined by the user
    ds.hirni_import_dcm(path=filename, session='user_defined_session')

    subs = ds.subdatasets(fulfilled=True, recursive=True, recursion_limit=None,
                          result_xfm='datasets')

    assert opj(ds.path, 'user_defined_session', 'dicoms') in [s.path for s in subs]
    ok_exists(opj(ds.path, 'user_defined_session', 'studyspec.json'))

    import os
    ok_exists(opj(ds.path, 'user_defined_session', 'dicoms', 'structural'))

    # now import again, but let the import routine figure out a session name
    # based on DICOM metadata (ATM just the first occurring PatientID, I think)
    ds.hirni_import_dcm(path=filename, session=None)

    subs = ds.subdatasets(fulfilled=True, recursive=True, recursion_limit=None,
                          result_xfm='datasets')

    assert opj(ds.path, 'sub-02', 'dicoms') in [s.path for s in subs]
    ok_exists(opj(ds.path, 'sub-02', 'studyspec.json'))
    ok_exists(opj(ds.path, 'sub-02', 'dicoms', 'structural'))

