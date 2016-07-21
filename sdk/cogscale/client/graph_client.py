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

from cogscale.client.results import Success, create_error

DEFAULT_CYPHER_PATH = "db/data/cypher"
C12E_GRF_PATH = "c12e/grf/query"


class GraphClient(object):

    def __init__(self, client):
        self.client = client

    def query(self, query, params={}, depth=0):
        args = {'query': query, 'params': params}
        path = DEFAULT_CYPHER_PATH
        if depth > 0:
            path = C12E_GRF_PATH
            args['depth'] = depth

        r = self.client.post(path, args)
        if r.status_code == 200:
            return Success({'result': r.json()})

        return create_error(r)