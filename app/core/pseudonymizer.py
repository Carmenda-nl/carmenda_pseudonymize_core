# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Pseudonymization utilities for generating random string mappings.

This module provides functionality to create pseudonyms (random strings) for
unique names while maintaining consistent mappings.
"""

import random
import string


def pseudonymize(unique_names, pseudonym_key=None, droplist=[], logger=None):
    """
    Generates pseudonyms (random strings) for a list of unique names.

    Parameters:
        unique_names (list): List of names to pseudonymize
        pseudonym_key (dict, optional): Existing name-pseudonym mapping. Default: None
        droplist (list, optional): Names to skip. Default: []
        logger (logging.Logger, optional): Logger for information messages. Default: None

    Returns:
        dict: Mapping of original names to pseudonyms. Also contains None:None mapping.

    Raises:
        AssertionError: If number of unique names doesn't match number of mappings.
    """
    unique_names = [name for name in unique_names if name not in droplist]

    if pseudonym_key is None:
        logger.info('Building new key because argument was None')
        pseudonym_key = {}
    else:
        logger.info('Building from existing key')

    for name in unique_names:
        if name not in pseudonym_key:
            found_new = False
            iterate = 0

            while found_new is False and iterate < 15:
                iterate += 1
                pseudonym_candidate = ''.join(
                    random.choices(string.ascii_uppercase + string.ascii_uppercase + string.digits, k=14)
                )
                if pseudonym_candidate not in pseudonym_key.values():
                    found_new is True
                    pseudonym_key[name] = pseudonym_candidate

    error_message = 'Unique_names (input) and pseudonym_key (output) do not have the same length'
    assert len(unique_names) == len(pseudonym_key.items()), error_message

    return pseudonym_key
