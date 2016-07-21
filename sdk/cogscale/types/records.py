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
from datetime import datetime
from uuid import uuid4
from cogscale.util.utils import json_encode


class ErrorRecord:
    def __init__(self, msg, data):
        self.error = msg
        self.data = data


class Record:
    def __init__(self, data, mapping, security, brand_info):
        """
        Data Record Output by Agents
        :param data: any object with a do_dict method
        :type data: object
        :param mapping: mapping name suggested for projecting data
        :type mapping: str
        :param security: security info classifying authorization of data
        :type security: SecurityInfo
        :param brand_info: brand description of the source of the data
        :type brand_info: BrandInfo
        """
        self.data = data
        self.mapping = mapping
        self.security = security
        self.brand_info = brand_info
        self.timestamp = datetime.now()

    def to_json(self, indent=None):
        return json.dumps(self.to_ardrecord(), indent=indent, sort_keys=True, default=json_encode)

    def data_to_dict(self):
        if hasattr(self.data, 'to_dict'):
            return self.data.to_dict()

        # Let jsonpickle or other encoding handle
        return self.data

    def to_ardrecord(self):
        record = {
            'h': {
                '@rid': uuid4().get_hex(),
                '@mapping': self.mapping,
                '@timestamp': self.timestamp,
                '@security': self.security,
                '@brand': self.brand_info
            },
            'd': self.data_to_dict()
        }
        return record


class BrandInfo:

    def __init__(self, brand_id, agent_id, config):
        self.brand_id = brand_id
        self.agent_id = agent_id
        self.config = config


class SecurityInfo:
    def __init__(self, classification):
        self.classification = classification


class UnitOfWork:

    def __init__(self, key, value):
        self.key = key
        self.value = value
        self.result = None

    def to_dict(self):
        if self.result:
            d = self.result.copy()
            d.update(self.key)
            return d
        return {}


class PublishStatus:

    def __init__(self, uow, success=False, status=None):
        if not status:
            status = {}

        self.success = success
        self.status = status
        self.uow = uow

    def summarize(self):
        return {'status': self.status, 'success': self.success, 'key': self.uow.key['_keys']}