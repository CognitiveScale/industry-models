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

import datetime
""" generated source for module DomainFrame """
#
#  * @author dilum
#  * @since 8/27/14
#  
class DomainFrame(object):
    """ generated source for class DomainFrame """
    #  TODO - change this to a representation of time ranges
    #  TODO introduce Enum here: [<sun|mon|tue|wed|thu|fri|sat>]
    #  TODO - map this to google places api
    def __init__(self, startDt=None, endDt=None, startTOD=None, endTOD=None,
                 daysOfWeek=None, geo=None, values=None):
        """
        A domainframe represents a structured query
        :param startDt:
        :type startDt: datetime
        :param endDt:
        :type endDt: datetime
        :param startTOD:
        :type startTOD: str
        :param endTOD:
        :type endTOD: str
        :param daysOfWeek:
        :type daysOfWeek: list
        :param geo:
        :type geo: Geometry
        :param values:
        :type values: dict of [str, list]
        :return:
        """
        """ generated source for method __init__ """
        self.startDt = startDt
        self.endDt = endDt
        self.startTOD = startTOD
        self.endTOD = endTOD
        self.daysOfWeek = daysOfWeek
        self.geo = geo
        self.values = values

class Location():
    """ generated source for class Location """
    URI = "http://i6s.io/types/Location"
    def __init__(self, latitude, longitude):
        """
        :type latitude: float
        :param latitude:
        :type longitude: float
        :param longitude:
        :return:
        """
        self.latitude = latitude
        self.longitude = longitude

#
#  * @author msanchez at cognitivescale.com
#  * @since 3/18/14
#
class Geometry():
    """ generated source for class Geometry """
    URI = "http://i6s.io/types/Geometry"
    def __init__(self, location):
        """
        :type location: Location
        :param location:
        :return:
        """
        self.location = location