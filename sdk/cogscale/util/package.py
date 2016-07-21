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

from cogscale.agents.decorators import insight, source
import click
import sys
import os.path
import logging
import yaml
import tempfile
import contextlib, shutil, zipfile
import datetime
import requests

log = logging.getLogger()
formatter = logging.Formatter('%(asctime)s:%(module)s:%(threadName)s:%(message)s')
handler = logging.StreamHandler()
handler.setFormatter(formatter)
log.addHandler(handler)
log.setLevel(logging.INFO)


@contextlib.contextmanager
def tempdir(prefix='tmp'):
    """A context manager for creating and then deleting a temporary directory."""
    tmpdir = tempfile.mkdtemp(prefix=prefix)
    try:
        yield tmpdir
    finally:
        shutil.rmtree(tmpdir)

@click.command()
@click.argument("input_dir", type=click.Path(dir_okay=True, file_okay=False, exists=True))
@click.option("--inspect", type=click.Path(dir_okay=False, exists=True), help="generate an agents.yml file by inspecting an entry python file")
@click.option("--prefix", help="a prefix for the generated zipfile")
def package(input_dir, inspect, prefix=None):
    """This script packages the INPUT_DIR as an agent bundle for upload to the server"""

    with tempdir() as outd:
        builddir = os.path.join(outd, "build")
        shutil.copytree(input_dir, builddir)
        agents = []
        if inspect:
            entry = os.path.relpath(inspect, input_dir)
            agents_path = os.path.join(builddir, "agents.yml")
            if os.path.isfile(agents_path):
                raise Exception("agents.yml file already exists.  Use --no-inspect to avoid overwriting.")
            sys.path.append(input_dir)
            try:
                module_name = os.path.splitext(entry)[0].replace('/', '.')
                __import__(module_name)
            except StandardError, e:
                log.error("Unable to load %s as %s", entry, module_name, exc_info=True)
                sys.exit(1)
            for agent in insight.all.keys():
                agents.append({"type": "insight", "name": agent, "python": entry})
            for agent in source.all.keys():
                agents.append({"type": "source", "name": agent, "python": entry})
            if len(agents) == 0:
                raise Exception("No agents found in %s" % entry)
            with file(agents_path, mode="w") as f:
                yaml.dump_all(agents, f, default_flow_style=False)
        outputprefix = prefix if prefix is not None else "package"
        ts = datetime.datetime.utcnow()
        outputname = "%s-%s.zip" % (outputprefix,ts.strftime("%Y%m%d%H%M%S"))
        with zipfile.ZipFile(outputname, 'w', compression=zipfile.ZIP_DEFLATED) as zfile:
            cwd = os.getcwd()
            os.chdir(builddir)
            for root, dirs, files in os.walk("."):
                for f in (possible for possible in files if not possible.lower().endswith(".pyc")):
                    zfile.write(os.path.join(root, f))
            os.chdir(cwd)

@click.command()
@click.argument("package", type=click.Path(dir_okay=False, exists=True))
@click.option("--server", default="localhost:3125", help="Specify a server to upload the package to.  (include port)")
def upload(package, server):
    """This script uploads the PACKAGE created by the package command to the service catalog"""
    url = "http://%s/upload/binary" % server
    files = {'file': (package, open(package, 'rb'))}
    try:
        log.info("uploading package to %s" % server)
        requests.post(url, files=files)
    except:
        log.error("Unable to upload package", exc_info=True)
        raise


if __name__ == "__main__":
    package()