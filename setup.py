#!/usr/bin/env python

from setuptools import setup
from setuptools import find_packages

setup(
    # basic project properties can be set arbitrarily
    name="datalad_cbbsimaging",
    author="CBBS imaging platform developers",
    author_email="michael.hanke@gmail.com",  # TODO establish project email
    version='0.0.1',
    description="DataLad extension for CBBS imaging platform workflows",
    packages=[pkg for pkg in find_packages('.') if pkg.startswith('datalad')],
    # datalad command suite specs from here
    install_requires=[
        'datalad-neuroimaging',
    ],
    entry_points = {
        'datalad.extensions': [
            # the label in front of '=' is the command suite label
            # the entrypoint can point to any symbol of any name, as long it is
            # valid datalad interface specification (see demo in this extensions
            'cbbsimaging=datalad_cbbsimaging:command_suite',
        ]
    },
)
