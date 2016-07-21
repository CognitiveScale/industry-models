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

import os
from pathlib import Path
from cogscale.agents.environment import AgentEnvironment
from cogscale.agents.decorators import insight as insight_func, source as source_func, enrichment as enrich_func, train as train_func, predict as predict_func, publish as publish_func
from cogscale.agents.frame import DomainFrame
from cogscale.util.pipe_runner import PipeRunner, SourcingProcess, LearningProcess, EnrichmentProcess, BaseProcess, PublishProcess, PredictionProcess, MessagePipe
from cogscale.util.encoder import Iso8601Handler
from cogscale.types.records import UnitOfWork
from cogscale.types.models import Model
from cogscale.util.utils import denormalize
from cogscale.util.service_clients import DssClient, ModelRegistryClient
from cogscale.util.parsers import AgentsParser
import click
from os.path import splitext
import yaml
import logging
import sys
from datetime import datetime
import json
from jsonpickle import handlers as json_handlers
from jsonpickle import encode
from Queue import Queue

log = logging.getLogger()
formatter = logging.Formatter('%(asctime)s:%(module)s:%(threadName)s:%(message)s')
handler = logging.StreamHandler()
handler.setFormatter(formatter)
log.addHandler(handler)
log.setLevel(logging.INFO)

# all json serialization should use Iso8601Handler
json_handlers.register(datetime, Iso8601Handler)


@click.group()
def cli():
    pass


@cli.command()
@click.option("--requests", type=click.File())
@click.option("--query")
@click.option("--verbose", is_flag=True)
@click.option("--config", help="Json string representing the activation configuration.")
@click.argument("python_file_or_module", type=click.Path(file_okay=True, dir_okay=False, readable=True))
@click.argument("name")
def insight(python_file_or_module, name, config, requests, query=None, verbose=False):
    if verbose:
        log.setLevel(logging.DEBUG)
    load_module(python_file_or_module)
    context = {"config": json.loads(config)} if config is not None else dict()
    environment = AgentEnvironment(name, insight_func, initial_context=context)
    request_frames = []
    if requests is not None:
        request_frames = yaml.load_all(requests)
    else:
        log.debug("Building frame from query %s" % query)
        datetime_now = datetime.datetime.now()
        # TODO: make --query --requests exclusive group
        request_frames.append(
            DomainFrame(datetime_now, datetime_now + datetime.timedelta(days=1), None, None, None, None,
                        {"rawQuery": {"name": "UNTYPED", "value": query}}))
    environment.setup()
    for frame in request_frames:
        log.info("Sending frame %s" % json.dumps(json.loads(encode(frame)), indent=4))
        environment_run = environment.run(domainframe=frame)
        log.info("Received insight(s) %s" % json.dumps(json.loads(encode(environment_run)), indent=4))
    environment.teardown()


@cli.command()
@click.option("--requests", type=click.File())
@click.option("--data", type=click.File(), help="Data to pass to the agent.  Used en lieu of a dss query")
@click.option("--dss", help="specify a Data Subscription Service to call for input data")
@click.option("--output", help="Output file path")
@click.option("--verbose", is_flag=True)
@click.option("--config", help="Json string representing the activation configuration.")
@click.option("--config-file", help="File containing activation configuration.")
@click.argument("python_file_or_module", type=click.Path(file_okay=True, dir_okay=False, readable=True))
@click.argument("name")
def insight2(python_file_or_module, name, config, requests, dss=None, data=None, model=None, config_file=None,
             output=None,
             verbose=False):
    if verbose:
        log.setLevel(logging.DEBUG)
    load_module(python_file_or_module)
    context = load_config("insights", name, config, config_file)

    environment = AgentEnvironment(name, insight_func, initial_context=context)
    request_records = []
    if requests is not None:
        request_json = json.load(requests)
        request_records = map(lambda obj: UnitOfWork(obj['key'], obj['value']), request_json.get("objects"))

    if data is not None:
        data_payload = load_data(data)
    else:
        data_payload = dict()

    # TODO - load and pass context
    config_payload = process_config(context, dss)
    payload = dict(config_payload.items() + data_payload.items())
    environment.setup()

    output = output or "insights.json"
    count = 0
    with open(output, "w") as f:
        log.info("Storing insight agent output in %s" % output)
        insights = environment.run(**payload)
        for i in insights:
            count += 1
            f.write(encode(i, unpicklable=False))
            f.write('\n')
    environment.teardown()
    log.info("Generated %d insight(s) and stored output in %s" % (count, output))


@cli.command()
@click.option("--output", help="Output file path", type=click.File(mode='w'), default="records.json")
@click.option("--verbose", is_flag=True)
@click.option("--config", help="Json string representing the activation configuration.")
@click.option("--config-file", help="File containing activation configuration.")
@click.option("--pipe",
              help="operate in pipe mode receiving commands on STDIN and sending output to the --output option",
              is_flag=True)
@click.option("--limit", help="Limit the number of records retrieved.", type=click.INT)
@click.argument("python_file_or_module", type=click.Path(file_okay=True, dir_okay=False, readable=True))
@click.argument("name")
def source(python_file_or_module, name, pipe, config=None, config_file=None, limit=None, output=None, verbose=False):
    if verbose:
        log.setLevel(logging.DEBUG)
    load_module(python_file_or_module)

    environment = AgentEnvironment(name, source_func, initial_context=dict())
    process = SourcingProcess(environment)

    if pipe:
        pipe = MessagePipe()
    else:
        log.info("Storing sourcing agent output in %s" % output)
        context = load_config("sources", name, config, config_file)
        pipe = HarnessPipe(output_file=output)
        pipe.send_start({"config": context, "limit": limit})
        def wait_for_complete():
            process.is_shutdown_ready(timeout=None)
            pipe.send_stop()
        from threading import Thread
        Thread(target=wait_for_complete, name="wait_for_sourcing").start()

    PipeRunner(pipe=pipe, process=process).start()


@cli.command()
@click.option("--data", type=click.File(), help="specify a file with data en lieu of using DSS")
@click.option("--output", help="Output file path", type=click.File(mode='w'), default="enrichments.json")
@click.option("--verbose", is_flag=True)
@click.option("--pipe",
              help="operate in pipe mode receiving commands on STDIN and sending output to STDOUT",
              is_flag=True)
@click.option("--workspace", type=click.Path(exists=True, file_okay=False, dir_okay=True, readable=True, writable=True),
              help="Path of existing workspace for model data")
@click.option("--dss", help="specify a Data Subscription Service to call for input data")
@click.option("--config", help="Json string representing the activation configuration.")
@click.option("--config-file", help="File containing activation configuration.")
@click.option("--limit", help="Limit the number of enriched records.", type=click.INT)
@click.argument("python_file_or_module", type=click.Path(file_okay=True, dir_okay=False, readable=True))
@click.argument("name")
def enrich(python_file_or_module, name, config, data, pipe, workspace, dss=None, config_file=None, limit=None,
           output=None,
           verbose=False):
    if verbose:
        log.setLevel(logging.DEBUG)
    load_module(python_file_or_module)
    environment = AgentEnvironment(name, enrich_func, initial_context=dict())

    if pipe:
        PipeRunner(process=EnrichmentProcess(environment)).start()
    else:
        context = load_config("enrichments", name, config, config_file, workspace)
        environment.context.update(context)
        queries = AgentsParser().get_bindings(name)
        if len(queries) != 1:
            raise Exception("Enrichment doesn't currently support more than one data stream as input.")
        data_key = queries[0]
        if data is not None:
            data_payload = {data_key: json.load(data).get("objects", list())}
        else:
            data_payload = dict()

        payload = dict(process_config(context, dss).items() + data_payload.items())

        log.info("Storing enrichment agent output in %s" % output)

        class nonlocal:
            counter = 0

        process = EnrichmentProcess(environment)
        process.run(HarnessPipe(output_file=output), config=context, limit=limit, **payload)
        # TODO - update this to use status
        log.info("Enriched %d records" % nonlocal.counter)


@cli.command()
@click.option("--data", type=click.File(), help="Data to pass to the agent.  Used en lieu of a dss query")
@click.option("--pipe",
              help="operate in pipe mode receiving commands on STDIN and sending output to the --output option",
              is_flag=True)
@click.option("--dss", help="specify a Data Subscription Service to call for input data", default="localhost:8090")
@click.option("--workspace",
              type=click.Path(file_okay=False, dir_okay=True),
              default="./",
              help="Model output directory path. Used for persisting model state")
@click.option("--verbose", is_flag=True)
@click.option("--config", help="Json string representing the activation configuration.")
@click.option("--config-file", help="File containing activation configuration.")
@click.argument("python_file_or_module", type=click.Path(file_okay=True, dir_okay=False, readable=True))
@click.argument("name")
def train(python_file_or_module, name, config, pipe, data, dss, config_file=None, workspace=None, verbose=False):
    if verbose:
        log.setLevel(logging.DEBUG)

    load_module(python_file_or_module)
    environment = AgentEnvironment(name, train_func, initial_context=dict())

    if not pipe:
        workspace = Path(workspace) if workspace else Path.cwd()
        if not workspace.exists():
            os.mkdir(str(workspace))
        context = load_config("learnings", name, config, config_file, str(workspace))

        if data is None:
            # attempt to query dss for data
            # find all querybindings in agents.yml:
            payload = process_queries(context, dss)
        else:
            payload = load_data(data)

        log.info("Storing model output in %s" % str(workspace))
        pipe = HarnessPipe()
        pipe.send_start(dict(payload.items() + {"config": context}.items()))
        pipe.send_stop()
    else:
        pipe = MessagePipe()

    PipeRunner(pipe=pipe, process=LearningProcess(environment)).start()

@cli.command()
@click.option("--data", type=click.File(), help="specify a json file holding an array of data to send to the prediction function.")
@click.option("--dss", help="specify a Data Subscription Service to call for input data", default="localhost:3380")
@click.option("--query", help="specify a Data Subscription Service to call for input data")
@click.option("--workspace", type=click.Path(exists=True, file_okay=False, dir_okay=True, readable=True, writable=True),
              help="Path of existing workspace for model data", default="/tmp")
@click.option("--model", help="specify a model url to load a model from a model registry.  Used with --registry")
@click.option("--registry", help="specify a model registry host and port from which to load models.  Used with --model", default="localhost:3125")
@click.option("--output", help="Output file path", type=click.File(mode='w'), default="-")
@click.option("--verbose", "-v", is_flag=True)
@click.option("--pipe",
              help="operate in pipe mode receiving commands on STDIN and sending output to the --output option",
              is_flag=True)
@click.option("--limit", help="Limit the number of predicted records.", type=click.INT)
@click.argument("python_file_or_module", type=click.Path(file_okay=True, dir_okay=False, readable=True))
@click.argument("name")
def predict(python_file_or_module, name, data, workspace, query=None, dss=None, registry=None, model=None, limit=None, output=None, verbose=False, pipe=False):
    if verbose:
        log.setLevel(logging.DEBUG)

    load_module(python_file_or_module)
    environment = AgentEnvironment(name, predict_func, initial_context=dict())

    if not pipe:
        if model is None and len(os.listdir(workspace)) != 0: # load models from workspace
            pass
        elif model is not None:
            (slug,timestamp) = model.split(":", 1)
            if slug is None and timestamp is None:
                raise Exception("Invalid model url for slug %s and timestamp %s", (slug, timestamp))
            model_client = ModelRegistryClient(registry)
            metadata = model_client.retrieve_model(slug, timestamp, workspace)
            log.info("Loaded model to %s with metadata: %s" % (workspace, metadata))
        else:
            raise Exception("Either --model must be specified or --workspace must contain a model")

        if data is None and query is not None:
            # attempt to query dss for data
            # find all querybindings in agents.yml:
            payload = process_queries({"body": "query: %s" % query}, dss)
        elif data is not None and query is None:
            payload = load_data(data, "body")
        else:
            raise Exception("Either --query or --data must be specified but not both.")

        log.info("Storing prediction agent output in %s" % output)
        pipe = HarnessPipe(output_file=output)
        pipe.send_start({"command": payload, "config": {Model.MODEL_WORKSPACE_KEY: workspace}, "limit": limit})
        pipe.send_stop()

    else:
        pipe = MessagePipe()

    PipeRunner(pipe=pipe, process=PredictionProcess(environment)).start()


@cli.command()
@click.option("--data", type=click.File())
@click.option("--verbose", is_flag=True)
@click.option("--output", help="Output file path", type=click.File(mode='w'), default="status.json")
@click.option("--pipe",
              help="operate in pipe mode receiving commands on STDIN and sending output to STDOUT",
              is_flag=True)
@click.option("--dss", help="specify a Data Subscription Service to call for input data")
@click.option("--config", help="Json string representing the activation configuration.")
@click.option("--config-file", help="File containing activation configuration.")
@click.option("--limit", help="Limit the number of enriched records.")
@click.argument("python_file_or_module", type=click.Path(file_okay=True, dir_okay=False, readable=True))
@click.argument("name")
def publish(python_file_or_module, name, config, data, pipe, limit=None, output=None, dss=None, config_file=None,
            verbose=False):

    if verbose:
        log.setLevel(logging.DEBUG)

    load_module(python_file_or_module)
    environment = AgentEnvironment(name, publish_func, initial_context=dict())

    if not pipe:
        context = load_config("destinations", name, config, config_file)
        environment.context.update(context)
        queries = AgentsParser().get_bindings(name)
        if len(queries) != 1:
            raise Exception("Destination doesn't currently support more than one data stream as input.")
        data_key = queries[0]
        if data is not None:
            data_payload = {data_key: json.load(data).get("objects", list())}
        else:
            data_payload = dict()

        payload = dict(process_config(context, dss).items() + data_payload.items())

        log.info("Storing destination agent output in %s" % output)
        pipe = HarnessPipe(output_file=output)
        pipe.send_start(dict(payload.items() + {"config": context, "limit": limit}.items()))
    else:
        pipe = MessagePipe()

    PipeRunner(pipe=pipe, process=PublishProcess(environment)).start()


def load_module(python_file_or_module):
    sys.path.append(".")
    module_name = splitext(python_file_or_module)[0].replace('/', '.')
    try:
        log.info("Importing %s", module_name)
        __import__(module_name)
    except StandardError, e:
        log.error("Unable to load %s as %s", python_file_or_module, module_name, exc_info=True)
        sys.exit(1)


def load_config(service_type, name, config, config_file, workspace=None):
    # assume there is an agents.yml file in the cwd
    try:
        with open("agents.yml", "r") as a:
            agents_yml = yaml.load(a)
    except Exception:
        log.warn("No agents.yml file found.  No default config settings will be used.")
        agents_yml = dict()
    default_config = {k: v.get("default") for k, v in
                      agents_yml.get(service_type, dict()).get(name, dict()).get("config_template", dict()).items()
                      if "default" in v.keys()}
    context = json.loads(config) if config is not None else dict()
    if not config and config_file:
        with open(config_file) as f:
            context = json.load(f)
    if workspace is not None:
        context[Model.MODEL_WORKSPACE_KEY] = workspace

    return dict(default_config.items() + context.items())


def process_config(config, dss, params=dict()):
    results = dict()
    results.update(process_models(config))
    results.update(process_queries(config, dss, params))
    return results


def process_models(config):
    results = dict()
    workspace_root = config.get("workspace")
    model_keys = BaseProcess.keys_with_value_type(config, "model")
    for (name, path) in [(k, v[6:].strip()) for k, v in config.iteritems() if (k in model_keys)]:
        model_path = "%s/%s/model.bin" % (workspace_root, path)
        workspace_path = "%s/%s/workspace" % (workspace_root, path)
        results[name] = Model.load(model_path, workspace_path)
    return results


def process_queries(config, dss, context=dict()):
    payload = dict()
    if dss is None:
        raise Exception("Dss endpoint is required if fetching data using a query.")
    client = DssClient(dss)
    for name, query in [(k, v[6:]) for k, v in config.items() if str(v).startswith("query:")]:
        results = client.execute_query(query, params=context)
        payload.update({name: results.get("results")})
    return payload

def find_type(config, type):
    BaseProcess.keys_with_value_type(config, type)

def load_data(data, param="data"):
    raw_data = json.load(data)
    if isinstance(raw_data, dict):
        # agent harness supports passing in a list of dictionaries (as the pipeRunner would call the training function)
        # or a single dictionary of lists which the harness will convert into the above list assigned to the 'data'
        # key.
        payload = {param: denormalize(raw_data)}
    else:
        payload = {param: raw_data}
    return payload


class HarnessPipe(object):
    import sys
    def __init__(self, output_file=None):
        self.output = output_file or sys.stdout
        self.commands = Queue()

    def on_data(self, obj):
        self.output.write(encode(obj, unpicklable=False))
        self.output.write("\n")

    def send(self, obj):
        if obj.get("response") == "DATA":
            self.on_data(obj.get("payload"))

    def receive(self):
        return self.commands.get(block=True)

    def send_start(self, payload):
        self.commands.put({"request":"START", "payload": payload})

    def send_stop(self, timeout=None):
        self.commands.put({"request":"STOP", "payload": {"timeout": timeout}})

if __name__ == "__main__":
    cli()
