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

from decorators import setup, teardown
from cogscale.exceptions.config_exception import InvalidAgentException
import logging

log = logging.getLogger()


class AgentEnvironment(object):
    def __init__(self, name, agent_type, initial_context={"config": dict()}):
        self.name = name
        self.setup_func = self._find_func(name, setup)
        self.agent_func = self._find_func(name, agent_type)
        if self.agent_func is None:
            raise InvalidAgentException("No function found decorated with %s named %s" % (agent_type.__name__, name))
        self.teardown_func = self._find_func(name, teardown)
        self.context = initial_context
        log.debug("AgentEnvironment: setup[%s], run[%s], teardown[%s]" % (self.setup_func, self.agent_func, self.teardown_func))

    def _find_func(self, agent_name, func_type):
        return func_type.all.get(agent_name, func_type.all.get("__DEFAULT__"))

    def setup(self):
        if self.setup_func is not None:
            self.setup_func(self.context)

    def teardown(self):
        if self.teardown_func is not None:
            self.teardown_func(self.context)

    def run(self, *args, **kwargs):
        # filter passed arguments down to only those the agent supports
        import inspect
        agent_sig = inspect.getargspec(self.agent_func)
        agent_args = {key: kwargs[key] for key in kwargs if key in agent_sig[0]}

        return self.agent_func(self.context, *args, **agent_args)

