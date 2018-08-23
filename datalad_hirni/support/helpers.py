
# TODO: Probably we need some proper SpecHandler class or sth, where this needs
# to go


def sort_spec(spec):
    """Helper to provide a key function for `sorted`

    Provide key to sort by type first and by whatever identifies a particular
    type of spec dict

    Parameters
    ----------
    spec: dict
      study specification dictionary

    Returns
    -------
    string
    """

    if spec['type'] == 'dicomseries':
        return 'dicomseries' + spec['uid']
    else:
        # ATM assuming everything else is identifiable by its location:
        return spec['type'] + spec['location']
