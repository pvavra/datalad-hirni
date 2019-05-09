# move spec functions here (from heuristic for ex)

# TODO: Probably we need some proper SpecHandler class or sth, where this needs
# to go

# Also: heuristic to resources?


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


def get_specval(spec, key):
    return spec[key]['value']


def has_specval(spec, key):
    return key in spec and 'value' in spec[key] and spec[key]['value']
