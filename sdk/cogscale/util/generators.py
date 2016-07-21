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

from multiprocessing import Queue


class BlockingQueueGenerator(object):

    """
    Provides a generator that utilizes a bounded, blocking queue to store objects.  Useful for async sourcing agents
    that need provide a stream of objects over time as a generator.
    """

    def __init__(self, maxsize=1000):
        self._data = Queue(maxsize=maxsize)

    def __iter__(self):
        return self

    def __next__(self):
        return self.next()

    def next(self):
        return self._data.get(block=True)

    def emit(self, obj):
        self._data.put_nowait(obj)