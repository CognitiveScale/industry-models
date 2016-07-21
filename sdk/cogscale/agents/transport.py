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

""" generated source for module InsightTransportResponse """
import uuid

from cogscale.agents.frame import DomainFrame


class InsightTransportResponse(object):
    def __init__(self, agentType=None, frame=None, common=None, specifics=None, insightId=uuid.uuid4().hex,
                 evidence=None):
        '''
        :param agentType:
        :type agentType: str
        :param frame_:
        :type frame_: DomainFrame
        :param common:
        :type common: Common
        :param specifics:
        :type specifics: object
        :param evidence:
        :type evidence: list of Evidence
        '''
        self.agentType = agentType
        self.frame = frame
        self.common = common
        self.specifics = specifics
        self.insightId = insightId
        self.evidence = evidence


class Common(object):
    def __init__(self, person=None, place=None, product=None, title=None, subtitle=None):
        """
        Holds common insight types
        :param person:
        :type person: str
        :param place:
        :type place: str
        :param product:
        :type product: str
        :param title:
        :type title: str
        :param subtitle:
        :type subtitle: str
        :return:
        """
        self.person = person
        self.place = place
        self.product = product
        self.title = title
        self.subtitle = subtitle


class Evidence(object):
    def __init__(self, evidenceId=uuid.uuid4().hex, polarity=0.0, polaritySource=None, concept=None, conceptSource=None,
                 text=None):
        """
        Evidence establishing
        :param evidenceId:
        :type evidenceId:  str
        :param polarity:
        :type polarity:  float
        :param polaritySource:
        :type polaritySource: str
        :param concept:
        :type concept:  str
        :param conceptSource:
        :type conceptSource: str
        :param text:
        :type text: str
        :return:
        """
        self.id = evidenceId
        self.polarity = polarity
        self.polaritySource = polaritySource
        self.concept = concept
        self.conceptSource = conceptSource
        self.text = text
