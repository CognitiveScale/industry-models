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

from cogscale.util.utils import is_sequence


class BaseField(object):

    name = None

    def __init__(self, default=None):
        self.default = default
        self.value = None

    def __get__(self, instance, owner):
        if instance is None:
            return self

        if self.name in instance.__dict__:
            return instance.__dict__[self.name]

        return None

    def __set__(self, instance, value):
        if value is None and self.default is not None:
            value = self.default
            if callable(value):
                value = value()

        instance.__dict__[self.name] = value
        # print 'value set:', self.name, instance.__dict__[self.name]

    def __delete__(self, instance):
        pass

    def validate(self, value):
        pass


class StringField(BaseField):

    def __init__(self):
        BaseField.__init__(self)

    def validate(self, value):
        return isinstance(value, basestring)


class IndexKeyField(BaseField):

    def __init__(self):
        BaseField.__init__(self)

    def validate(self, value):
        return isinstance(value, basestring)


class IntegerField(BaseField):

    def __init__(self, default=None):
        BaseField.__init__(self, default)

    def validate(self, value):
        try:
            int(value)
        except TypeError:
            return False
        return True


class LongField(BaseField):

    def __init__(self, default=None):
        BaseField.__init__(self, default)

    def validate(self, value):
        try:
            long(value)
        except TypeError:
            return False
        return True


class FloatField(BaseField):

    def __init__(self, default=None):
        BaseField.__init__(self, default)

    def validate(self, value):
        try:
            float(value)
        except TypeError:
            return False
        return True


class BooleanField(BaseField):

    def __init__(self, default=False):
        BaseField.__init__(self, default)

    def validate(self, value):
        try:
            bool(value)
        except TypeError:
            return False
        return True


class EdgeField(BaseField):

    def __init__(self, name, inverse_name=None):
        BaseField.__init__(self)
        self.name = name
        self.value = []
        self.inverse_name = inverse_name

    def __get__(self, instance, owner):
        if instance is None:
            return self

        if self.name not in instance.__dict__:
            instance.__dict__[self.name] = []

        return instance.__dict__[self.name]

    def __set__(self, instance, value):
        if self.name not in instance.__dict__:
            instance.__dict__[self.name] = []

        if value is None:
            instance.__dict__[self.name] = []
        elif is_sequence(value):
            instance.__dict__[self.name] = value
        else:
            instance.__dict__[self.name].append(value)

        # print self.name, instance.__dict__[self.name]