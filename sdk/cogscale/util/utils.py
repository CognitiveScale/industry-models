#
# Copyright 2016 CognitiveScale, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from urlparse import urlparse
from datetime import datetime
from cogscale.util.attribute_getter import AttributeGetter
from collections import defaultdict


def is_url(s):
    """
    Determine if the given string is a valid URL
    """
    url = urlparse(s)
    if url.scheme:
        return True
    return False


def is_sequence(arg):
    """
    Determine if the given arg is a sequence type
    """
    return (not hasattr(arg, "strip") and
            hasattr(arg, "__getitem__") or
            hasattr(arg, "__iter__"))


def json_encode(obj):
    """
    An encoder function to use with the built-in json library in python.  The encoder framework in jsonpickle is likely
    a better choice, but this function will provide some reasonable behavior if that is not available.
    """

    # Look for a to_dict method and use it if available
    if hasattr(obj, 'to_dict'):
        return obj.to_dict()

    # Support for converting AttributeGetter objects to dict
    if isinstance(obj, AttributeGetter):
        d = obj.__dict__.copy()
        d.pop('_setattrs')
        for prop in d:
            if prop.startswith("__"):
                d.pop(prop)
        return d

    # Encode datetime objects using ISO standard
    if isinstance(obj, datetime):
        return obj.isoformat()

    # Fallback to string representation
    return repr(obj)


def denormalize(normalized_data):
    """
    Convert a dictionary of lists to a list of dictionaries
    :param normalized_data: in the form {a: [1,2,3...], b: [5,6,7...]}
    :return: [{a:1,b:5}, {a:2,b:6}, {a:3,b:7}...]
    """
    acc = defaultdict(dict)
    for k in normalized_data.keys():
        for i, v in enumerate(normalized_data[k]):
            acc[i][k] = v
    return acc.values()


def normalize(denormalized):
    """
    Convert a list of dictionaries into a dictionary of lists
    :param denormalized: in the form [{a:1,b:5}, {a:2,b:6}, {a:3,b:7}...]
    :return: {a: [1,2,3...], b: [5,6,7...]}
    """
    acc = defaultdict(list)
    for row in denormalized:
        for (k, v) in row.items():
            acc[k].append(v)
    return acc

