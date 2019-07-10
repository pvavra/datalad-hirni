.. _chap_specification:

Specification
*************

Overview
========

The specification of a study dataset captures metadata on data entities within that dataset and in particular is meant
to specify how exactly to convert them to a targeted data/layout standard (to BIDS). Technically this mechanism is by no
means limited to the BIDS standard but it was build with BIDS in mind.

By default this specification is distributed throughout several files with the default name ``studyspec.json`` in the
dataset's root directory as well as each acquisition directory. However, not only can you change the default name (via
`configuration variable <chap_customization>`_ ``datalad.hirni.studyspec.filename``), but you can also add an arbitrary
number of such files at arbitrary locations within the dataset. This is because any command that would touch those files
can be given a path to the file(s) to consider. Furthermore, the conversion can be restricted to particular
specification files and within them to a particular type of entities to convert. It might therefore be desirable to
distribute the specification in a more fine-grained fashion. Last but not least this might also ease the generation and
editing of those files.

As the name suggests, the files themselves are basically JSON files except they are what is called a JSON stream. This
means, that each line in the files is valid JSON dictionary, which is what we call a specification snippet. Each of
those dictionaries is the specification of a single data entity.
Because of that, these files can be processed on a per-line basis, which for example allows to ignore some error in one
snippet and proceed with the next one, instead of needing to evaluate the entirety of the content only to be able to
start processing the actual content, which would be the case if the file was plain JSON (that is: one single structure,
in that case a list of dictionaries). However, this comes at a cost: Some standard tools/libraries for handling JSON
might not be able to deal with those files out of the box.

*TODO*
    - point out, how exactly those specifications are used and link to respective sections on conversion and customization
    - add a few words regarding the term "data entities"

Structure
=========

*TODO*
    - required fields
    - approve flag
    - (semi-)autogeneration
    - tags, procedures
    - arbitrary additions
