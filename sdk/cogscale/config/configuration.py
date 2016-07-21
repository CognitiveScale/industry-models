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
import json
from os.path import expanduser

from cogscale.client import Client
from environment import Environment
from cogscale.exceptions import ConfigurationException


class Configuration(object):
    """
    A class representing the configuration of your Cognitive Scale account and environment.
    You must call configure before any other operations. ::

        cogscale.Configuration.configure(
            environment,
            "your_api_key"
        )
    """

    environment = None
    api_key = None

    @staticmethod
    def configure(environment, api_key=os.getenv("CS_API_KEY")):
        if not environment:
            raise ConfigurationException("Cognitive Scale environment must be specified")

        if not environment.server:
            raise ConfigurationException("Cognitive Scale server hostname or IP address is required")

        if not environment.port:
            raise ConfigurationException("Cognitive Scale server port is required")

        if not api_key:
            raise ConfigurationException("Cognitive Scale API key must be specified")

        Configuration.environment = environment
        Configuration.api_key = api_key

    @staticmethod
    def api_version():
        return "1"

    @staticmethod
    def instantiate():
        return Configuration(
            environment=Configuration.environment,
            api_key=Configuration.api_key
        )

    @staticmethod
    def configureFromLocal():
        home = expanduser('~')
        configFilePath = "%s/.cogscale" % home
        if os.path.isfile(configFilePath):
            with open(configFilePath) as configFile:
                conf = json.load(configFile)
                env = Environment(server=conf['server'], port=conf['port'], use_ssl=conf['use_ssl'])
                Configuration.configure(env, conf['api_key'])
        else:
            Configuration.environment = Environment(None, None, None)
            Configuration.api_key = None

    @staticmethod
    def client():
        return Client(Configuration.instantiate())

    def saveConfig(self):
        home = expanduser('~')
        with open(os.path.abspath("%s/.cogscale" % home), 'w') as configFile:
            config = {
                'api_key': Configuration.api_key,
                'server': Configuration.environment.server,
                'port': Configuration.environment.port,
                'use_ssl': Configuration.environment.use_ssl
            }

            configFile.write(json.dumps(config))

    def __init__(self, environment, api_key):
        self.environment = environment
        self.api_key = api_key