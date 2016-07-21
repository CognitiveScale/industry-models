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
from client import Client, RESTClient
from cogscale.config import Configuration, Environment
from cogscale.util.attribute_getter import AttributeGetter
from resource import Resource
from results import Success, Error
from dataset_client import DatasetClient
from graph_client import GraphClient
from service_client import ServiceClient

NEO4J_PORT = 7474
PROJECT_ADMIN_PORT = 3125


class Project(Resource):

    def __init__(self, client, attrs):
        Resource.__init__(self, client, attrs)

        if hasattr(self, 'stack'):
            self.stack = AttributeGetter(self.stack)

    @staticmethod
    def find_projects():
        """
        Find and list all projects the caller has access to.
        :return: a list of project objects
        """
        client = Configuration.client()
        r = client.get("projects")
        if r.status_code == 200:
            return Success({'projects': [Project(client, p) for p in r.json()['projects']]})
        else:
            return Error({'error': "Error finding projects - %d: %s" % (r.status_code, r.text)})

    @staticmethod
    def get_project(project_slug):
        """
        Gets a specific project for the specified slug.
        :param project_slug: the unique human readable identifier for the project to retrieve
        :return: a project object
        """
        client = Configuration.client()
        r = client.get("projects/%s" % project_slug)
        if r.status_code == 200:
            obj = r.json()
            proj_attrs = obj['project']
            stack_attrs = obj['stack']
            proj_attrs['stack'] = stack_attrs
            return Success({'project': Project(client, proj_attrs)})
        else:
            return Error({'error': "Error getting project with slug %s - %d: %s" %
                                   (project_slug, r.status_code, r.text)})

    def dataset(self):

        """
        An example of reading a dataset::

        prj = Project.get_project("my_project").project
        result = prj.dataset().read("customers", {
            "skip": 10,
            "limit": 10,
            "select": "name,address,phone_number",
            "sort": "name:desc"
        })

        if result.is_success:
            my_dataset = result.dataset
        """

        client = Client(self.__create_project_config())
        return DatasetClient(client)

    def graph(self):

        """
        Access and query the cognitive graph for the project.

        prj = Project.get_project("my_project").project
        result = prj.graph().query("...")
        """

        client = RESTClient(self.__create_graph_config().environment.base_url)
        return GraphClient(client)

    def services(self):

        """
        Provides the service client which is used to query the available services and activations
        in this project.
        :return: the service client
        """

        client = Client(self.__create_project_config())
        return ServiceClient(client)

    def __create_project_config(self):
        env = Environment(os.getenv("CS_NEPTUNE_HOST", self.slug + "." + Configuration.environment.server),
                          PROJECT_ADMIN_PORT, Configuration.environment.use_ssl)
        return Configuration(env, Configuration.api_key)

    def __create_graph_config(self):
        env = Environment(os.getenv("NEO4J_HOST", self.slug + "." + Configuration.environment.server),
                          NEO4J_PORT, Configuration.environment.use_ssl)
        return Configuration(env, None)
