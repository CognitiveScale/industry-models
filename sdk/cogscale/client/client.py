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

import os
import requests
import json
from cogscale.exceptions.client_exception import ClientException


class RESTClient(object):

    """
    A generic client that facilitates easy communication with RESTful services.
    """

    def __init__(self, base_url, debug=os.getenv('CS_DEBUG', False)):
        self.base_url = base_url
        self.debug = debug

    def get(self, path, params={}, headers={}):
        if self.debug:
            print "GET", "%s/%s" % (self.base_url, path), "params=", params
        return requests.get("%s/%s" % (self.base_url, path), params=params, headers=headers)

    def get_as_json(self, path, params={}, headers={}):
        if self.debug:
            print "GET", "%s/%s" % (self.base_url, path), "params=", params

        headers['content-type'] = 'application/json'
        r = requests.get("%s/%s" % (self.base_url, path), params=params, headers=headers)
        return self._toJsonOrError(r)

    def getJson(self, path, params={}, headers={}):
        return self.get_as_json(path, params, headers)

    def put(self, path, obj, params={}, headers={}):
        if self.debug:
            print "PUT", "%s/%s" % (self.base_url, path), json.dumps(obj)
        return requests.put("%s/%s" % (self.base_url, path), data=json.dumps(obj), params=params, headers=headers)

    def post(self, path, obj, params={}, headers={}):
        if self.debug:
            print "POST", "%s/%s" % (self.base_url, path), json.dumps(obj)
        return requests.post("%s/%s" % (self.base_url, path), data=json.dumps(obj), params=params, headers=headers)

    def delete(self, path, params={}, headers={}):
        if self.debug:
            print "DELETE", "%s/%s" % (self.base_url, path), "params=", params

        return requests.delete("%s/%s" % (self.base_url, path), params=params, headers=headers)

    def _toJsonOrError(self, r):
        if r.status_code == 200:
            return r.json()
        else:
            print r.text
            raise ClientException("Server returned status code %d" % r.status_code)


class Client(RESTClient):

    """
    A Cognitive Scale REST client that automatically adds required API key params and headers.
    """

    def __init__(self, configuration, debug=os.getenv('CS_DEBUG', False)):
        env = configuration.environment
        self.api_key = configuration.api_key
        RESTClient.__init__(self, "%s/api/v%s" % (env.base_url, configuration.api_version()), debug)

    def get(self, path, params={}, headers={}):
        headers.update({'x-cogscale-authorization': self.api_key})
        params["key"] = self.api_key
        return RESTClient.get(self, path, params, headers)

    def put(self, path, obj, params={}, headers={}):
        headers.update({'content-type': 'application/json', 'x-cogscale-authorization': self.api_key})
        params["key"] = self.api_key
        return RESTClient.put(self, path, obj, params, headers)

    def post(self, path, obj, params={}, headers={}):
        headers.update({'content-type': 'application/json', 'x-cogscale-authorization': self.api_key})
        params["key"] = self.api_key
        return RESTClient.post(self, path, obj, params, headers)