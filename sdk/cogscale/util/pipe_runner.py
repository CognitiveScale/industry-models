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

import logging
from threading import Thread, Event, Lock
import sys
import signal
import json
import traceback

from jsonpickle import encode

from cogscale.agents.environment import AgentEnvironment
from cogscale.types.records import UnitOfWork
from cogscale.types.models import Model


class SourcingStatus(object):
    def __init__(self, running=True):
        self.currentState = "running"
        self.running = running
        self.completed = False
        self.successCount = 0
        self.excludedCount = 0
        self.errorText = None
        self.details = None


'''
@startuml
[*] --> polling
state polling {
StatusRequest --> StatusResponse
state "call source function" as starting
DataResponse --> DataResponse : iterate over records
DataResponse --> FatalError : exception thrown
FatalError : set errorText
FatalError --> [*]
DataResponse --> Teardown : Generator Completes
Teardown : mark status complete=True
Teardown --> [*]
StartRequest --> starting
starting --> DataResponse
stopping: * exit DataResponse Loop
stopping: * set running = False
StopRequest --> stopping
stopping --> StopResponse : shutdownComplete = True
}
@enduml
'''

SHUTDOWN_TIMEOUT_SECONDS = 25.0
SIGINT_SHUTDOWN_TIMEOUT_SECONDS = 5.0
FATAL_ERROR_CODE = "FATAL"


class MessagePipe(object):
    def __init__(self, input=sys.stdin, output=sys.stdout):
        self.log = logging.getLogger()
        self.output = output
        self.input = input
        self.sending_lock = Lock()

    def send(self, obj):
        with self.sending_lock:
            msg = encode(obj, unpicklable=False)
            self.log.debug("sending %s" % msg)
            print >> self.output, msg
            self.output.flush()

    def receive(self):
        userinput = self.input.readline()
        userinput = userinput.rstrip('\n')
        self.log.debug("received %s" % userinput)
        return json.loads(userinput)


class BaseProcess(object):
    def __init__(self, status=SourcingStatus()):
        self.log = logging.getLogger()
        self.shutdown_requested = Event()
        self.shutdown_requested.clear()
        self.shutdown_ready = Event()
        self.shutdown_ready.clear()
        self.status = status
        self.status_lock = Lock()
        self.model_cache = dict()

    def request_shutdown(self):
        self.shutdown_requested.set()

    def shutdown_in_progress(self):
        return self.shutdown_requested.is_set() and not self.shutdown_ready.is_set()

    def is_shutdown_ready(self, timeout):
        return self.shutdown_ready.wait(timeout)

    def get_status(self):
        return self.status

    def run(self, *args, **kwargs):
        raise NotImplementedError("Please Implement this method")

    def shutdown(self):
        pass

    def should_send_status(self):
        return True

    def inc_success_count(self):
        with self.status_lock:
            self.status.successCount += 1

    def inc_excluded_count(self):
        with self.status_lock:
            self.status.excludedCount += 1

    def fatal_status(self, msg):
        self.log.error(msg, exc_info=True)
        self.status.errorCode = FATAL_ERROR_CODE
        self.status.details = traceback.format_exc()

    @staticmethod
    def keys_with_value_type(config, type_name):
        """
        Find keys that have a value with the specified type
        :param config: dict()
        :param type_name: str name of type to match on
        :return: list of str

        A config might hold native key/value pairs or keys with rich values (i.e. {"type":"query", "value":"BIND(a,b,c) as x"}).
        This method finds all keys that have that type.
        """

        def is_type_of(type_name, value):
            if isinstance(value, dict):
                return value.get("type", "").lower() == type_name.lower()
            else:
                return str(value).startswith("%s:" % type_name)

        return [k for (k, v) in config.iteritems() if is_type_of(type_name, v)]

    @staticmethod
    def normalize_config(config):
        """
        Normalize configs with rich value types to key/value pairs
        :param config: dict of key/rich value pairs
        :return: dict of key/simple value pairs
        Agents expect config to be a dictionary of key to python value pairs.  Richer types cannot be represented by
         native JSON types (like a model which is made up of a slug and timestamp). To allow for this, config value
         objects are used to specify a type and value.  For native types (string, string[], number, number[], date),
         the native type is stored in the value field (i.e. {type: "string", value: "some string value"}).  For richer
         types, the entire object is returned (i.e. {type: "model", model: "someslug", timestamp: "20140101T12:13:13-06:00"}).
         This method converts all native value types back to a key/value pair so existing agents don't have to be modified.
        """
        richTypes = ["query", "model"]

        def transform_value(input):
            if (isinstance(input, dict) and not input.get("type") in richTypes):
                try:
                    return input["value"]
                except KeyError, e:
                    return input
            else:
                return input

        return {k: transform_value(v) for (k, v) in config.iteritems()}

    def get_or_update_model(self, name, pickle):
        if name not in self.model_cache:
            self.model_cache[name] = Model.deserialize(pickle)
        return self.model_cache.get(name)

    def transform_models(self, config, payload):
        """
        Transform model blobs in payload into python objects
        :param config: configuration for agent
        :param payload: existing payload with model blobs
        :return: a new copy of the payload with models converted to Model objects
        """
        model_keys = self.keys_with_value_type(config, "model")

        def transform(k, value):
            if k in model_keys and not isinstance(value, Model):
                return self.get_or_update_model(k, value)
            else:
                return value

        return {k: transform(k, v) for (k, v) in payload.iteritems()}


class PredictionProcess(BaseProcess):
    def __init__(self, environment):
        super(PredictionProcess, self).__init__()
        self.environment = environment
        self.is_setup = False

    def run(self, pipe, **kwargs):
        try:
            self.status.completed = False
            self.shutdown_ready.clear()
            config = kwargs.pop("config")
            limit = kwargs.get("limit")
            command = kwargs.pop("command", dict())
            token = command.get("token", dict())
            source_data = command.pop("body")
            self.environment.context.update(config)
            self.log.info("launching prediction with %d records" % len(source_data))
            if self.environment is not None and not self.is_setup:
                self.environment.setup()
                self.is_setup = True
            self.status.running = True
            early_exit = False
            counter = 1
            predictions = []
            for (index, value) in enumerate(source_data):
                if limit is not None and counter > limit:
                    break
                try:
                    prediction = self.environment.run(value)
                    predictions.append(prediction)
                    counter += 1
                except Exception, e:
                    self.log.warn("Unable to process prediction %s" % value)
                    pipe.send({"response": "SKIPPED", "payload": {"reason": "error", "details": traceback.format_exc(), "token": token, "job_value": value, "index": index}})
            if len(predictions) > 0:
                pipe.send({"response": "DATA", "payload": {"token": token, "body": predictions}})
        except Exception, e:
            self.fatal_status("Unable to predict")
            early_exit = True

        if not early_exit:
            self.status.completed = True

        self.shutdown_ready.set()

    def shutdown(self):
        if self.environment is not None:
            self.environment.teardown()
        super(PredictionProcess, self).shutdown()


class EnrichmentProcess(BaseProcess):
    def __init__(self, environment):
        super(EnrichmentProcess, self).__init__()
        self.environment = environment
        self.record_id_key = "_keys"
        self.is_setup = False

    def to_key(self, keys):
        return {self.record_id_key: keys}

    def process_records(self, pipe, records, counter, limit):
        for record in records:
            counter += 1
            if self.shutdown_requested.isSet():
                return True
            self.inc_success_count()
            payload = record.to_ardrecord()
            pipe.send({"response": "DATA", "payload": payload})
            if limit and counter >= limit:
                self.request_shutdown()
        return False

    def run(self, pipe, **kwargs):
        try:
            self.status.completed = False
            self.shutdown_ready.clear()
            # TODO - implement model as a service client [PLAT-560]
            config = self.normalize_config(kwargs.pop("config", dict()))
            limit = kwargs.pop("limit", None)
            data_key = self.keys_with_value_type(config, "query")[0]
            if len(self.keys_with_value_type(config, "model")) > 0:
                raise Exception("Models are not currently supported in enrichments")
            kwargs = self.transform_models(config, kwargs)
            self.log.info("launching enrichment agent with %s" % config)
            self.status.running = True
            early_exit = False
            # enrichments are called by unit of work so we must know the data key that holds the source of UOW's
            source_data = kwargs.pop(data_key)
            uow_s = [UnitOfWork(self.to_key(obj['key']), obj['value']) for obj in source_data]
            self.environment.context.update(config)
            if not self.is_setup:
                self.environment.setup()
                self.is_setup = True
            counter = 0
            for uow in uow_s:
                if self.shutdown_requested.isSet():
                    early_exit = True
                    break
                kwargs[data_key] = uow
                records = self.environment.run(**kwargs)
                early_exit = self.process_records(pipe, records, counter, limit)
        except Exception, e:
            self.fatal_status("Unable to enrich")
            early_exit = True

        if not early_exit:
            self.status.completed = True

        self.shutdown_ready.set()

    def shutdown(self):
        self.environment.teardown()
        super(EnrichmentProcess, self).shutdown()


class LearningProcess(BaseProcess):
    def __init__(self, environment):
        super(LearningProcess, self).__init__()
        self.environment = environment
        self.status = SourcingStatus()
        self.status_lock = Lock()

    def run(self, pipe, **kwargs):
        self.status.running = True
        self.shutdown_ready.clear()
        early_exit = False
        try:
            config = self.normalize_config(kwargs.pop("config", dict()))
            self.log.info("launching learning agent with %s" % config)
            self.environment.context.update(config)
            self.environment.setup()
            model_metadata = self.environment.run(**kwargs)
            pipe.send({"response": "DATA", "payload": {"metadata": model_metadata}})
        except Exception, e:
            self.fatal_status("Unable to call training")
            early_exit = True
        finally:
            self.environment.teardown()

        if not early_exit:
            self.status.completed = True

        self.shutdown_ready.set()


class SourcingProcess(BaseProcess):
    def __init__(self, environment):
        super(SourcingProcess, self).__init__(status=SourcingStatus())
        self.environment = environment

    def run(self, pipe, **kwargs):
        self.status.running = True
        self.shutdown_ready.clear()
        config = self.normalize_config(kwargs.pop("config", dict()))
        limit = kwargs.pop("limit", None)

        self.log.info("launching sourcing agent with %s" % config)
        if config:
            self.environment.context.update(config)

        self.environment.setup()
        early_exit = False
        records = self.environment.run()
        counter = 0
        try:
            for r in records:
                if self.shutdown_requested.isSet():
                    early_exit = True
                    break
                try:
                    pipe.send({"response": "DATA", "payload": r.to_ardrecord()})
                    self.inc_success_count()
                except:
                    # eventually pipe error records to separate stream for correction
                    self.inc_excluded_count()
                counter += 1
                if limit and counter >= limit:
                    break
        except Exception, e:
            early_exit = True
            self.fatal_status("An exception occured while sourcing.")
        self.environment.teardown()
        if not early_exit:
            self.status.completed = True
        self.log.info("Sourced %d records" % self.status.successCount)
        self.shutdown_ready.set()


class PublishProcess(BaseProcess):
    def __init__(self, environment):
        super(PublishProcess, self).__init__()
        self.environment = environment
        self.record_id_key = "_keys"
        self.is_setup = False

    def to_key(self, keys):
        return {self.record_id_key: keys}

    def process_status(self, pipe, status, counter, limit):
        for st in status:
            counter += 1
            if self.shutdown_requested.isSet():
                return True
            if st.success:
                self.inc_success_count()
            if limit and counter >= limit:
                self.request_shutdown()
        return False

    def run(self, pipe, **kwargs):
        try:
            self.status.completed = False
            config = self.normalize_config(kwargs.pop("config", dict()))
            limit = kwargs.pop("limit", None)
            data_key = self.keys_with_value_type(config, "query")[0]
            self.log.info("launching destination agent with %s" % config)
            self.status.running = True
            early_exit = False
            source_data = kwargs.pop(data_key)
            uow_s = [UnitOfWork(self.to_key(obj['key']), obj['value']) for obj in source_data]
            self.environment.context.update(config)
            if not self.is_setup:
                self.environment.setup()
                self.is_setup = True
            counter = 0
            # for obj in source_data:
            for uow in uow_s:
                if self.shutdown_requested.isSet():
                    early_exit = True
                    break
                kwargs[data_key] = uow
                status = self.environment.run(**kwargs)
                early_exit = self.process_status(pipe, status, counter, limit)
        except Exception, e:
            self.fatal_status("Unable to publish")
            early_exit = True

        if not early_exit:
            self.status.completed = True

        if self.shutdown_in_progress():
            self.shutdown_ready.set()

    def shutdown(self):
        self.environment.teardown()
        super(PublishProcess, self).shutdown()


class PipeRunner(object):
    def __init__(self, pipe=MessagePipe(), process=None):
        """
        :param environment:
        :type environment: AgentEnvironment
        :param output:
        :return:
        """
        if process is None:
            raise Exception("a process must be specified")
        self.process = process
        self.pipe = pipe
        self.log = logging.getLogger()
        self.agent_thread = None

        def handle_signal(signal, frame):
            self.process.request_shutdown()
            self.log.warn("received shutdown signal.")
            self.process.is_shutdown_ready(SIGINT_SHUTDOWN_TIMEOUT_SECONDS)
            self.log.info("exiting.")

        signal.signal(signal.SIGINT, handle_signal)
        self.send({"response": "READY"})

    def start(self):
        self.log.info("Started Polling")
        while True:
            request = self.receive()
            if self.dispatch_or_exit(request):
                break
        self.log.info("exiting polling loop")

    def dispatch_or_exit(self, request):
        """
        Process a request from the pipe
        :param request: {request:, payload:} object
        :return: boolean - True if ready to shutdown
        """
        requestType = request.get("request")
        if requestType == "STATUS":
            status = self.process.get_status()
            self.send_status(status)
        elif (requestType == "START"):
            self.start_command(request.get("payload", dict()))
        elif requestType == "STOP" and not self.process.shutdown_in_progress():
            return self.stop_command(request.get("payload", dict()).get("timeout"))
        else:
            logging.warn("unexpected request")
        return False

    def start_command(self, payload):
        def wrapper():
            self.process.run(self.pipe, **payload)
            if self.process.should_send_status():
                status = self.process.get_status()
                self.send_status(status)

        self.agent_thread = Thread(target=wrapper, name="agent_runner")
        self.agent_thread.daemon = True
        self.agent_thread.start()

    def stop_command(self, timeout=SHUTDOWN_TIMEOUT_SECONDS):
        self.process.request_shutdown()
        self.process.is_shutdown_ready(timeout=timeout)
        self.process.shutdown()
        self.send({"response": "STOPPED"})
        return True

    def send_status(self, status):
        self.send({"response": "STATUS", "payload": status})

    def is_shutdown(self, timeout):
        return self.process.is_shutdown_ready(timeout)

    def send(self, obj):
        self.pipe.send(obj)

    def receive(self):
        return self.pipe.receive()
