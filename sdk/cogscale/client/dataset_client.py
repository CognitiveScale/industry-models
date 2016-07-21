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

from dataset import DatasetReader
from results import Error, Success


class DatasetClient(object):

    def __init__(self, client):
        self.client = client

    def find_datasets(self):
        r = self.client.getJson("datasets")
        if "datasets" in r:
            return Success({"datasets": r["datasets"]})

        return Error({"error": "Error listing datasets: %s" % r.error})

    def read(self, dataset_slug, params={}):
        r = self.client.getJson("datasets/%s" % dataset_slug, params)
        if "data" in r:
            return Success({"dataset": DatasetReader(dataset_slug, params, r, self)})

        return Error({"error": "Error reading Dataset %s: %s" % (dataset_slug, r.error)})

    def read_cursor(self, dataset_slug, params={}):
        r = self.client.getJson("datasets/%s" % dataset_slug, params)
        if "data" in r:
            return Success({"data": r})

        return Error({"error": "Error reading Dataset %s: %s" % (dataset_slug, r.error)})