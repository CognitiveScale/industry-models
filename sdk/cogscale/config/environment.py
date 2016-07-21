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


class Environment(object):
    """
    A class representing which environment the client library is using.
    Pass in one of the pre-defined values as the first argument to
    :class:`cogscale.config.Configuration.configure() <cogscale.config.configuration.Configuration>` ::

        cogscale.config.Environment.Development
        cogscale.config.Environment.Sandbox
        cogscale.config.Environment.Production

    Alternatively, you can create your own Environment instance for use with custom/private environments.
    """

    def __init__(self, server=os.getenv("CS_SERVER"), port=os.getenv("CS_PORT"), use_ssl=os.getenv("CS_SECURE")):
        self.__server = server
        self.__port = port
        self.use_ssl = use_ssl

    @property
    def base_url(self):
        return "%s://%s:%s" % (self.protocol, self.server, self.port)

    @property
    def port(self):
        return int(self.__port)

    @property
    def protocol(self):
        return self.use_ssl and "https" or "http"

    @property
    def server(self):
        return self.__server

    @property
    def server_and_port(self):
        return self.__server + ":" + self.__port


Environment.Development = Environment(os.getenv("CS_SERVER") or "localhost", os.getenv("CS_PORT") or "3000", False)
Environment.Sandbox = Environment("sandbox.insights.ai", "443", True)
Environment.Production = Environment("www.insights.ai", "443", True)