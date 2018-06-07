"""Procedure to apply a sensible BIDS default setup to a dataset
"""

import sys
import os.path as op
from datalad.distribution.dataset import require_dataset

# bound dataset methods
import datalad.distribution.add

ds = require_dataset(
    sys.argv[1],
    check_installed=True,
    purpose='BIDS dataset setup')

README_code = """\
All custom code goes into the directory. All scripts should be written such
that they can be executed from the root of the dataset, and are only using
relative paths for portability.
"""

# unless taken care of by the template already, each item in here
# will get its own .gitattributes entry to keep it out of the annex
# give relative path to dataset root (use platform notation)
force_in_git = [
    'README',
    'CHANGES',
    'dataset_description.json',
]

###################################################################
to_add = []

# amend gitattributes
for path in force_in_git:
    abspath = op.join(ds.path, path)
    d = op.dirname(abspath)
    ga_path = op.join(d, '.gitattributes') \
        if op.exists(d) else op.join(ds.path, '.gitattributes')
    with open(ga_path, 'a') as gaf:
        gaf.write('{} annex.largefiles=nothing\n'.format(
            op.relpath(abspath, start=d) if op.exists(d) else path))
    to_add.append(ga_path)

# leave clean
ds.add(
    to_add,
    message="Default BIDS dataset setup",
)
