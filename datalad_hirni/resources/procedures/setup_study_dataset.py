"""Procedure to apply a sensible default setup to a study dataset
"""

import sys
from datalad.distribution.dataset import require_dataset

# bound dataset methods
import datalad.distribution.add
import datalad.interface.save
from datalad.plugin.add_readme import AddReadme

ds = require_dataset(
    sys.argv[1],
    check_installed=True,
    purpose='study dataset setup')


force_in_git = [
    'README',
    'CHANGES',
    'dataset_description.json',
    '**/{}'.format(ds.config.get("datalad.hirni.studyspec.filename",
                                 "studyspec.json")),
]

# except for hand-picked global metadata, we want anything
# to go into the annex to be able to retract files after
# publication
ds.repo.set_gitattributes([('**', {'annex.largefiles': 'anything'})])
ds.repo.set_gitattributes([(p, {'annex.largefiles': 'nothing'})
                           for p in force_in_git])


# TODO:
# Note: This default is using the DICOM's PatientID as the acquisition ID
# (directory name in the study dataset). That approach works for values
# accessible via the DICOM metadata directly. We probably want a way to apply
# more sophisticated rules, which could be achieved by a String Formatter
# providing more sophisticated operations like slicing (prob. to be shared with
# datalad's --output-format logic) or by apply specification rules prior to
# determining final location of the imported subdataset. The latter might lead
# to a mess, since import and specification routines would then be quite
# twisted.
ds.config.add('datalad.hirni.import.acquisition-format',
              "{PatientID}", where='dataset')

ds.save(message='[HIRNI] Default study dataset setup')

# Include the most basic README to prevent heudiconv from adding one
ds.add_readme(filename='README', existing='fail')


# TODO: Reconsider using an import container and if so, link it herein. See
# now-deprecated hirni-create-study command
