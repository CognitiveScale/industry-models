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

from pathlib import Path
from cogscale.util.attribute_getter import AttributeGetter
from cloud.serialization import cloudpickle
import base64
import pickle
import tempfile
from os import path


def default_accessor(m):
    return m.predict


def default_setup(model_wrapper, workspace):
    pass



class Model(AttributeGetter):
    MODEL_WORKSPACE_KEY = "@modelDir"

    def __init__(self, model_id, model, accessor_func=default_accessor, setup_func=default_setup, attributes={}):
        self.model_id = model_id
        self.model = model
        self.accessor_func = accessor_func
        self.model_predict = None
        self.setup_func = setup_func
        AttributeGetter.__init__(self, attributes)

    def setup(self, workspace):
        self.setup_func(self, workspace)

    def predict_single(self, x):
        many = self.predict_many([x])
        return next(many)

    def predict_many(self, x_s):
        if self.model_predict is None:
            self.model_predict = self.accessor_func(self.model)
        return (self.model_predict(x) for x in x_s)

    @classmethod
    def dumps(cls, model):
        return cloudpickle.dumps(model)

    @classmethod
    def dump(cls, model, workspace_root, model_name="model.bin"):
        with open(path.join(workspace_root, model_name), mode="w") as model_file:
            model_file.write(cls.dumps(model))

    @classmethod
    def load(cls, workspace_path, model_name="model.bin"):
        with open(path.join(workspace_path, model_name), "r") as model_file:
            model_str = model_file.readlines()
            model_bin = cls.loads("".join(model_str))
        return model_bin

    @classmethod
    def loads(cls, model_str):
        model = cloudpickle.loads(model_str)
        return model


class ModelMetadata(AttributeGetter):
    def __init__(self, config, attributes={}):
        self.config = config
        super(ModelMetadata, self).__init__(attributes)


class ModelScore:
    def __init__(self, score, positive_factors = list(), negative_factors = list()):
        self.score = score
        self.positive_factors = positive_factors
        self.negative_factors = negative_factors