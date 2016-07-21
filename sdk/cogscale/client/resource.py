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


def id_str(obj, property):
    if not hasattr(obj, property):
        return None

    id_dict = getattr(obj, property)

    if not "$value" in id_dict:
        return str(id_dict)

    return id_dict["$value"]


class Resource(AttributeGetter):

    def __init__(self, client, attributes):
        AttributeGetter.__init__(self, attributes)
        self.client = client

    @property
    def id(self):
        return id_str(self, "_id")

