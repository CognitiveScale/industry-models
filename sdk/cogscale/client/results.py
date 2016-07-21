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

from cogscale.util.attribute_getter import AttributeGetter


class Success(AttributeGetter):
    """
    An instance of this class is returned from most operations when the request is successful.  Call the name
    of the resource (eg, dataset, project, etc) to get the object::

        result = Dataset.read({..})
        if result.is_success:
            dataset = result.dataset
        else:
            print result.error
    """

    @property
    def is_success(self):
        """ Returns whether the result from the server is a successful response. """
        return True


class Error(AttributeGetter):
    """
    An instance of this class is returned from most operations when there is a validation error.
    """

    @property
    def is_success(self):
        """ Returns whether the result from the server is a successful response. """
        return False


def create_error(r):
    msg = None
    try:
        err_json = r.json()
        msg = err_json["error"]
    except ValueError:
        pass
    return Error({"error": "Server returned status code %d: %s" % (r.status_code, msg or r.text)})


def create_error_from_json(j):
    return Error({"error": "Server returned error %s" % j["error"]})