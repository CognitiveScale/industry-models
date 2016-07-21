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

from requests import Session
import json
import zipfile
import sys
import StringIO


class DssClient(object):
    def __init__(self, baseurl):
        self.endpoint = "http://%s/api/v1/subscriptions/adhoc" % baseurl
        self.session = Session()

    def execute_query(self, query, params=dict()):
        query_payload = json.dumps({"query": query, "context": params})
        response = self.session.post(self.endpoint, data=query_payload, headers={"content-type": "application/json"})
        response.raise_for_status()
        return response.json()


class ModelRegistryClient(object):
    def __init__(self, host_and_port):
        self.endpoint = "http://%s/api/v1/models/" % host_and_port
        self.session = Session()

    def retrieve_model(self, slug, timestamp, destination):
        response = self.session.get("%s/%s/%s/default/model.bin" % (self.endpoint, slug, timestamp))
        response.raise_for_status()
        data = StringIO.StringIO(response.content)
        z = zipfile.ZipFile(data)
        z.extractall(destination)
        metadata = self.session.get("%s/%s/%s/default/metadata" % (self.endpoint, slug, timestamp))
        metadata.raise_for_status()
        return metadata.json()