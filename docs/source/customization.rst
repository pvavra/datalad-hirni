.. _chap_customization:

Customization
*************

There are a lot of ways to customize datalad-hirni. Some things are just a matter of configuration settings, while
others involve a few lines of (python) code.

Configuration
=============

As a DataLad extension, datalad-hirni uses DataLad's
`config mechanism <https://datalad.readthedocs.io/en/latest/config.html>`_. It just adds some additional variables. If
you look for a possible configuration to change some specific behaviour of the commands, refer also to the help pages
for those commands. Please don't hesitate to file an issue on
`GitHub <https://github.com/psychoinformatics-de/datalad-hirni>`_ if there's something you would like become
configurable as well.

**datalad.hirni.toolbox.url**
    This can be used to overwrite the default url to get the toolbox from. The url is then respected by the ``cfg_hirni``
    procedure. Please note, that therefore it will have no effect, if the toolbox was already installed into your
    dataset.

    This configuration may be used to refer to an offline version of hirni's toolbox or to switch to another toolbox
    dataset altogether.

**datalad.hirni.studyspec.filename**
    Use this configuration to change the default name for specification files (``studyspec.json``).

**datalad.hirni.dicom2spec.rules**
    Set this to point to a python file defining rules for how to derive a specification from DICOM metadata. (See below
    for more on implementing such rules). This configuration can be set multiple times, which will result in those rules
    overwriting each other. Therefore the order in which they are specified matters, with the later rules overwriting
    earlier ones. As with any DataLad configuration in general, the order of sources would be *system*, *global*,
    *local*, *dataset*. This could be used for having institution-wide rules via the system level, a scanner-based rule
    at the global level (of a specific computer at the scanner site), user-based and study-specific rules, each of which
    could either go with what the previous level decided or overwrite it.

**datalad.hirni.import.acquisition-format**
    This setting allows to specify a python format string, that will be used by ``datalad hirni-import-dcm`` if no
    acquisition name was given. It defines the name to be used for an acquisition (the directory name) based on DICOM
    metadata. The default value is ``{PatientID}``. Something that is enclosed with curly brackets will be replaced by
    the value of a variable with that name everything else is taken literally. Every field of the DICOM headers is
    available as such a variable. You could also combine several like ``{PatientID}_{PatientName}``.


Procedures
==========

Rules
=====
