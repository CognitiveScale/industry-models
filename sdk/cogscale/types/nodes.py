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

import json
from cogscale.types.fields import BaseField
from cogscale.types.graph import GraphBuilder
from cogscale.util.attribute_getter import AttributeGetter
from cogscale.util.utils import json_encode

_DEFAULT_TYPE_URI = 'http://i6s.io/types/SubGraph'


class Node(AttributeGetter):

    meta = {'searchable': False, 'type_uri': _DEFAULT_TYPE_URI}

    def __init__(self, *args, **kwargs):
        for key, val in self.__class__.__dict__.iteritems():
            if isinstance(val, BaseField):
                val.name = key

        AttributeGetter.__init__(self, kwargs)

    def __repr__(self, detail_list=None):
        detail_list = []
        for key, val in self.__class__.__dict__.iteritems():
            if isinstance(val, BaseField):
                detail_list.append(key)
        return AttributeGetter.__repr__(self, detail_list)

    def to_json(self, indent=None):
        return json.dumps(self, indent=indent, sort_keys=True, default=json_encode)

    def to_graph(self, space=None, correlation_id=None):
        b = GraphBuilder()
        return b.build_graph(self, space, correlation_id)

    def to_ardrecord(self, space=None, correlation_id=None):
        graph = self.to_graph(space, correlation_id)
        if 'type_uri' in self.meta:
            type_uri = self.meta['type_uri']
        else:
            type_uri = _DEFAULT_TYPE_URI

        return json.dumps({'_graph': graph, '_data': self, '_correlationId': graph.correlation_id, '_type': type_uri}, default=json_encode)