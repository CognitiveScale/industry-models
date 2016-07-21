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
from cogscale.client.resource import Resource
from cogscale.client.results import Success, create_error, Error


class Service(Resource):

    def __init__(self, client, attributes):
        Resource.__init__(self, client, attributes)


class Activation(Resource):

    def __init__(self, client, attributes):
        Resource.__init__(self, client, attributes)


class ServiceClient(object):

    def __init__(self, client):
        self.client = client

    def find_services_of_type(self, service_type):
        r = self.client.get_as_json("services", {'typeExpr': service_type})
        if "services" in r:
            return Success({"services": [Service(self.client, s) for s in r["services"]]})

        return Error({"error": "Error listing services of type %s: %s" % (service_type, r)})

    def get_service_of_type(self, service_type, service_id):
        r = self.client.get_as_json("services", {'typeExpr': service_type, 'idExpr': service_id})
        if "services" in r:
            if len(r["services"]) == 0:
                return Error({"error": "Service of type %s with ID %s not found" % (service_type, service_id)})
            return Success({"service": Service(self.client, r["services"][0])})

        return create_error(r)

    def find_activations(self):
        r = self.client.get_as_json("activations")
        if "activations" in r:
            return Success({'activations': [Activation(self.client, a) for a in r["activations"]]})
        return Error({"error": "Error listing activations: %s" % r})

    def get_activation(self, slug):
        r = self.client.get_as_json("activations/%s" % slug)
        return Success({'activation': Activation(self.client, r)})

    def activate_service(self, activation):
        r = self.client.post('activations', activation)
        if r.status_code == 201:
            if os.getenv("CS_DEBUG"):
                print r.headers
            slug = r.headers['Location']
            return Success({'message': 'Created activation %s' % slug, 'slug': slug})
        return create_error(r)

    def save_activation(self, slug, activation):
        if os.getenv("CS_DEBUG"):
            print slug, activation
        r = self.client.put('activations/%s' % slug, activation)
        if r.status_code == 200:
            return Success({'message': 'Saved activation %s' % slug})
        return create_error(r)

    def disable_activation(self, slug):
        r = self.client.put('activations/%s/state' % slug, "disabled")
        if r.status_code // 100 == 2:
            return Success({'message': 'disabled activation %s' % slug})
        return create_error(r)

    def resume_activation(self, slug):
        r = self.client.put('activations/%s/state' % slug, "enabled")
        if r.status_code // 100 == 2:
            return Success({'message': 'resumed activation %s' % slug})
        return create_error(r)

    def drop_activation(self, slug):
        r = self.client.delete('activations/%s' % slug)
        if r.status_code == 200:
            return Success({'message': 'Activation %s dropped successfully' % slug})
        return create_error(r)

    def service_status(self):
        r = self.client.get_as_json('status')
        if 'status' in r:
            return Success({'status': r['status']})
        return Error({'error': 'Error getting service status: %s' % r})