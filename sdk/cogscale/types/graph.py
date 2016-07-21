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
import hashlib
from threading import Lock
from uuid import uuid4
from cogscale.types.fields import BaseField, IndexKeyField, EdgeField
from cogscale.util.attribute_getter import AttributeGetter
from cogscale.util.utils import is_sequence


NODE_ID_PROPERTY = "correlationId"


class Locator(AttributeGetter):

    def __init__(self, **kwargs):
        AttributeGetter.__init__(self, kwargs)


class NamedProperties(object):

    """
    Base class for nodes and edges which both can contain named properties in a property graph model.
    """

    def __init__(self):
        self._properties = {}

    def add(self, name, value):
        self._properties[name] = value

    def get(self, name):
        if name in self._properties:
            return self._properties[name]
        return None

    def remove(self, name):
        del self._properties[name]

    def iterprops(self):
        return self._properties.iteritems()


class FutureNode(NamedProperties):

    """
    A node to be created or merged into the property graph.
    """

    def __init__(self, graph, nodes, alias, locator=None):
        NamedProperties.__init__(self)
        self.alias = alias
        self.graph = graph
        nodes[alias] = self
        self._labels = set()
        self.locator = locator

    def add_label(self, label):
        self._labels.add(label)

    def edge_to(self, other_node, rel_type):
        self.graph.establish_edge(self, other_node, rel_type)

    def to_dict(self):
        return {'alias': self.alias, 'labels': [label for label in self._labels], 'locator': self.locator, 'updates': self._properties}


class FutureEdge(NamedProperties):

    """
    An edge to be created or merged into the property graph.
    """

    def __init__(self, graph, edges, alias, locator):
        NamedProperties.__init__(self)
        self.graph = graph
        self.edges = edges
        self.alias = alias
        self.locator = locator
        edges[alias] = self

    def to_dict(self):
        return {'alias': self.alias, 'locator': self.locator, 'updates': self._properties}


class FutureSubGraph(object):

    """
    A collection of nodes and edges that are ready to be inserted or merged into a property graph.
    """

    def __init__(self, correlation_id, space=None):
        self.nodes = {}
        self.edges = {}
        self.space = space
        self.local_unique = 0
        self.local_unique_lock = Lock()
        self.correlation_id = correlation_id

    def insert_node(self):
        return FutureNode(self, self.nodes, self._unique())

    def upsert_node(self, label, key, value):
        return FutureNode(self, self.nodes, self._unique(), Locator(label=label, key=key, value=value))

    def establish_edge(self, from_node, to_node, rel_type):
        return FutureEdge(self, self.edges, self._unique(), Locator(start=from_node.alias, end=to_node.alias, type=rel_type))

    def _unique(self):
        self.local_unique_lock.acquire()
        unique = self.local_unique
        self.local_unique += 1
        self.local_unique_lock.release()

        return str(unique)

    def to_dict(self):
        return {'nodes': self.nodes, 'edges': self.edges, 'space': self.space}

    def to_json(self, indent=None):
        def json_encode(obj):
            if hasattr(obj, 'to_dict'):
                return obj.to_dict()
            if isinstance(obj, AttributeGetter):
                d = obj.__dict__.copy()
                d.pop('_setattrs')
                return d
            return repr(obj)
        return json.dumps(self, sort_keys=True, default=json_encode, indent=indent)


class GraphBuilder(object):

    def build_graph(self, node, space=None, correlation_id=None):
        if correlation_id is None:
            correlation_id = str(uuid4())

        g = FutureSubGraph(correlation_id, space)
        self._create_node(g, node, correlation_id)

        return g

    def _fields_for_node(self, node):
        fields = {}
        for key, val in node.__class__.__dict__.iteritems():
            if isinstance(val, BaseField):
                value = None
                if key in node.__dict__:
                    value = node.__dict__[key]
                if value is not None:
                    fields[key] = AttributeGetter({'field': val, 'value': value})
        return fields

    def _index_for_node(self, node):
        index_key = None
        index_hash = None
        for field_name, field in node.__class__.__dict__.iteritems():
            if isinstance(field, IndexKeyField):
                field_value = unicode(node.__dict__[field_name]).encode('utf-8')
                if index_key is None:
                    index_key = field_name
                    if field_value:
                        index_hash = hashlib.sha256()
                        index_hash.update(field_value)
                else:
                    index_key += ":::" + field_name
                    if field_value:
                        if not index_hash:
                            index_hash = hashlib.sha256()
                        index_hash.update(field_value)
        return index_key, index_hash.hexdigest() if index_hash else None

    def _create_node(self, g, node, correlation_id):
        labels = self._node_labels(node)

        # Collect all node fields and index fields
        fields = self._fields_for_node(node)
        index_key, index_value = self._index_for_node(node)

        if index_key is None:
            future_node = g.insert_node()
        else:
            future_node = g.upsert_node(labels[0], index_key, index_value)

        # Assign correlation ID for traceability
        future_node.add(NODE_ID_PROPERTY, correlation_id)

        # Build out the sub-graph for this node
        self._hydrate_node(g, future_node, fields, correlation_id)

        return future_node

    def _hydrate_node(self, g, node, fields, correlation_id):
        for name, field_info in fields.iteritems():
            if isinstance(field_info.field, EdgeField):
                for edge_node in field_info.value:
                    edge_future_node = self._create_node(g, edge_node, correlation_id)
                    g.establish_edge(node, edge_future_node, field_info.field.name)
                    if field_info.field.inverse_name:
                        g.establish_edge(edge_future_node, node, field_info.field.inverse_name)
            else:
                node.add(name, field_info.value)

    def _node_labels(self, node):
        if 'labels' in node.meta:
            labels = node.meta['labels']
            if not is_sequence(labels) or len(labels) < 1:
                return [node.__class__.__name__]
            return labels

        return [node.__class__.__name__]
