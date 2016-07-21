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

from cogscale.util.attribute_getter import AttributeGetter


class DatasetReader:

    def __init__(self, slug, params, cursor, client):
        self.slug = slug
        self.params = params
        self.client = client
        self._read_page(cursor)
        self.index = 0
        self.cursor = None
        self.skip = 0
        self.num_rows = None
        self.total_rows = None
        self.page = None

    def __iter__(self):
        return self

    def next(self):
        if self.index < len(self.page):
            df = DataFrame(self.page[self.index])
            self.index += 1
            return df
        elif self._next_page():
            return self.next()
        else:
            raise StopIteration

    def _read_page(self, cursor):
        self.cursor = cursor
        print cursor
        self.skip = cursor["skip"] or 0
        self.num_rows = cursor["numRows"]
        self.total_rows = cursor["totalRows"]
        self.page = cursor["data"]

    def _next_page(self):
        skip = self.skip + self.num_rows
        print "skipping %d rows" % skip
        self.params["skip"] = skip
        result = self.client.read_cursor(self.slug, self.params)
        if result.is_success:
            self._read_page(result.data)
            return self.num_rows > 0
        return False

    def df(self, convert_objects=True):
        import pandas as pd
        df = pd.DataFrame(self.page)
        if convert_objects:
            return df.convert_objects(convert_numeric=True)
        return df


class DataFrame(AttributeGetter):

    """
    A class representing the data objects in a Cognitive Scale Dataset.  Integrates with the Pandas
    data analysis library (http://pandas.pydata.org/).

    Example usage::

    my_dataset = ...

    result = my_dataset.read()

    if result.is_success:
        data = result.data
        df = data.df()

    """