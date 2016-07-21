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

class AttributeGetter(object):
    def __init__(self, attributes={}):
        self._setattrs = []
        for key, val in attributes.iteritems():
            try:
                setattr(self, key, val)
                self._setattrs.append(key)
            except AttributeError as e:
                print "Error setting attribute %s=%s" % (str(key), str(val))
                raise e

    def __repr__(self, detail_list=None):
        if detail_list is None:
            detail_list = self._setattrs

        details = ", ".join("%s: %r" % (attr, getattr(self, attr))
                            for attr in detail_list
                            if hasattr(self, attr))
        return "<%s {%s} at %d>" % (self.__class__.__name__, details, id(self))