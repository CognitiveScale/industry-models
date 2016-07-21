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

import yaml


class AgentsParser(object):
    def __init__(self, agents_file="./agents.yml"):
        self.yaml = yaml.load(open(agents_file))

    def get_queries(self, agent_name, config):
        bindings = self.get_bindings(agent_name)
        return [(k, v) for k, v in config.items() if k in bindings]

    def get_bindings(self, agent_name):
        return [k for k, v in self.get_config(agent_name).items() if v.get('type') == "QueryBinding"]


    def get_config(self, agent_name):
        for t, agents in self.yaml.items():
            for name, agent in agents.items():
                if name == agent_name:
                    return agent.get("config_template")


