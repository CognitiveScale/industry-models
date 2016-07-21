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

from functools import wraps

def generate_named_decorator():
    lookup = dict()
    def base_decorator(portableName=None, **kwargs):
        def decorator(f):
            name = portableName if portableName is not None else "__DEFAULT__"
            lookup[name] = f
            wraps(f)
            return f
        return decorator
    base_decorator.all = lookup
    return base_decorator

insight = generate_named_decorator()

setup = generate_named_decorator()

teardown = generate_named_decorator()

rank = generate_named_decorator()

feedback = generate_named_decorator()

source = generate_named_decorator()

enrichment = generate_named_decorator()

train = generate_named_decorator()

predict = generate_named_decorator()

publish = generate_named_decorator()