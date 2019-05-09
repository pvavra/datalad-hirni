"""BIDS-specific helper functions"""


def apply_bids_label_restrictions(value):
    """Sanitize file names for BIDS.
    """
    # only alphanumeric allowed
    # => remove everything else

    if value is None:
        # Rules didn't find anything to apply, so don't put anything into the
        # spec.
        return None

    from six import string_types
    if not isinstance(value, string_types):
        value = str(value)

    import re
    pattern = re.compile('[\W_]+')  # check
    return pattern.sub('', value)
