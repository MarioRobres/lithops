#
# Copyright Cloudlab URV 2020
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os
import json
import time
import logging
import importlib
import requests
import shlex
from concurrent.futures import ThreadPoolExecutor

from lithops.utils import is_lithops_worker, create_handler_zip
from lithops.constants import REMOTE_INSTALL_DIR


logger = logging.getLogger(__name__)
LOCAL_FH_ZIP_LOCATION = os.path.join(os.getcwd(), 'lithops_standalone.zip')
PROXY_SERVICE_PORT = 8080


class StandaloneHandler:
    """
    A StandaloneHandler object is used by invokers and other components to access
    underlying standalone backend without exposing the implementation details.
    """

    def __init__(self, standalone_config):
        self.config = standalone_config
        self.backend_name = self.config['backend']
        self.runtime = self.config['runtime']
        self.is_lithops_worker = is_lithops_worker()

        self.start_timeout = self.config.get('start_timeout', 300)
        self.auto_dismantle = self.config.get('auto_dismantle')
        self.hard_dismantle_timeout = self.config.get('hard_dismantle_timeout')
        self.soft_dismantle_timeout = self.config.get('soft_dismantle_timeout')
        self.pull_runtime = self.config.get('pull_runtime', False)

        module_location = 'lithops.standalone.backends.{}'.format(self.backend_name)
        sb_module = importlib.import_module(module_location)
        self.StandaloneInstance = getattr(sb_module, 'StandaloneInstance')

        self.exec_mode = self.config.get('exec_mode', 'consume')
        self.instances = []

        vsi = self.StandaloneInstance(self.config[self.backend_name], public_vsi=True)
        self.instances.append(vsi)

        logger.debug("Standalone handler created successfully")

    def _is_instance_ready(self, instance):
        """
        Checks if the VM instance is ready to receive ssh connections
        """
        try:
            instance.get_ssh_client().run_remote_command('id')
        except Exception:
            return False
        return True

    def _wait_instance_ready(self, instance):
        """
        Waits until the VM instance is ready to receive ssh connections
        """
        ip_addr = instance.get_ip_address()
        logger.debug('Waiting VM instance {} to become ready'.format(ip_addr))

        start = time.time()
        while(time.time() - start < self.start_timeout):
            if self._is_instance_ready(instance):
                return True
            time.sleep(5)

        self.dismantle()
        raise Exception('VM readiness {} probe expired. Check your VM'.format(ip_addr))

    def _is_proxy_ready(self, instance):
        """
        Checks if the proxy is ready to receive http connections
        """
        try:
            if self.is_lithops_worker:
                url = "http://{}:{}/ping".format('127.0.0.1', PROXY_SERVICE_PORT)
                r = requests.get(url, timeout=1, verify=True)
                if r.status_code == 200:
                    return True
                return False
            else:
                cmd = 'curl -X GET http://127.0.0.1:8080/ping'
                out = instance.get_ssh_client().run_remote_command(cmd, timeout=2)
                data = json.loads(out)
                if data['response'] == 'pong':
                    return True
        except Exception:
            return False

    def _wait_proxy_ready(self, instance):
        """
        Waits until the proxy is ready to receive http connections
        """
        ip_address = instance.get_ip_address()
        logger.info('Waiting Lithops proxy to become ready for {}'.format(ip_address))

        start = time.time()
        while(time.time() - start < self.start_timeout):
            if self._is_proxy_ready(instance):
                return True
            time.sleep(2)

        self.dismantle()
        raise Exception('Proxy readiness probe expired for {}. Check your VM'.format(ip_address))

    def run_job(self, job_payload):
        """
        Run the job description against the selected environment
        """
        total_calls = job_payload['job_description']['total_calls']

        if self.exec_mode == 'create':
            # First VSI (entry point instance) is already created in __init__
            vsi_to_create = total_calls - 1

            for vsi_n in range(vsi_to_create): 
                vsi = self.StandaloneInstance(self.config[self.backend_name])
                self.instances.append(vsi)

            with ThreadPoolExecutor(vsi_to_create) as executor:
                executor.map(lambda vsi: vsi.create(), self.instances)

        # Entry point instance
        ep_instance = self.instances[0]
        ip_address = ep_instance.get_ip_address()

        logger.debug("Checking if entry point VSI {} is ready".format(ip_address))
        if not self._is_proxy_ready(ep_instance):
            logger.debug("Entry point VSI {} is not ready".format(ip_address))
            # if entry point VSI is topped, this means all VSIs are stopped
            with ThreadPoolExecutor(total_calls) as executor:
                executor.map(lambda vsi: vsi.start(), self.instances)
            # Wait only for the entry point instance
            self._wait_instance_ready(ep_instance)
            if self.exec_mode == 'create':
                self._setup_lithops(ep_instance)

        logger.debug('Entry point VSI {} ready. Running job'.format(ip_address))
        cmd = 'python3 /opt/lithops/invoker.py {}'.format(shlex.quote(json.dumps(job_payload)))
        out = ep_instance.get_ssh_client().run_remote_command(cmd)
        logger.debug(out)
        response = json.loads(out)

        act_id = response['activationId']
        logger.debug('Job invoked on {}. Activation ID: {}'.format(ip_address, act_id))

        return act_id

    def create_runtime(self, runtime):
        """
        Installs the proxy and extracts the runtime metadata and
        preinstalled modules
        """
        ep_instance = self.instances[0]
        ip_addr = ep_instance.get_ip_address()
        logger.debug('Checking if VM instance is ready {}'.format(ip_addr))
        if not self._is_instance_ready(ep_instance):
            logger.debug('VM Instance not ready {}'.format(ip_addr))
            ep_instance.create()
            ep_instance.start()
            self._wait_instance_ready(ep_instance)
        self._setup_lithops(ep_instance)
        self._wait_proxy_ready(ep_instance)

        logger.debug('Extracting runtime metadata information')
        payload = {'runtime': runtime, 'pull_runtime': self.pull_runtime}
        cmd = ('curl http://127.0.0.1:8080/preinstalls -d {} '
               '-H \'Content-Type: application/json\' -X GET'
               .format(shlex.quote(json.dumps(payload))))
        out = ep_instance.get_ssh_client().run_remote_command(cmd)
        runtime_meta = json.loads(out)

        return runtime_meta

    def dismantle(self):
        """
        Stop all VM instances
        """
        logger.info("Entering dismantle for length {}".format(len(self.backends)))
        for instance in self.instances:
            logger.debug("Dismantle {} for {}".format(instance.get_instance_id(), instance.get_ip_address()))
            instance.stop()

    def clean(self):
        pass

    def clear(self):
        pass

    def get_runtime_key(self, runtime_name):
        """
        Wrapper method that returns a formated string that represents the
        runtime key. Each backend has its own runtime key format. Used to
        store modules preinstalls into the storage
        """
        return self.instances[0].get_runtime_key(runtime_name)

    def _setup_lithops(self, ep_instance):
        """
        Setup lithops necessary files and dirs in all VSIs using the entry point instance
        """
        ip_address = ep_instance.get_ip_address()
        logger.debug('Installing Lithops in VM instance {}'.format(ip_address))
        ssh_client = ep_instance.get_ssh_client()

        # Upload local lithops version to remote VM instance
        src_proxy = os.path.join(os.path.dirname(__file__), 'proxy.py')
        create_handler_zip(LOCAL_FH_ZIP_LOCATION, src_proxy)
        current_location = os.path.dirname(os.path.abspath(__file__))
        ssh_location = os.path.join(current_location, '..', 'util', 'ssh_client.py')
        setup_location = os.path.join(current_location, 'setup.py')

        logger.debug('Uploading lithops files to entry point VSI {}'.format(ip_address))
        files_to_upload = [(LOCAL_FH_ZIP_LOCATION, '/tmp/lithops_standalone.zip'),
                           (ssh_location, '/tmp/ssh_client.py'.format(REMOTE_INSTALL_DIR)),
                           (setup_location, '/tmp/setup.py'.format(REMOTE_INSTALL_DIR))]

        ssh_client.upload_multiple_local_files(files_to_upload)
        os.remove(LOCAL_FH_ZIP_LOCATION)

        # Create dirs and upload config
        cmd = 'rm -R {0}; mkdir -p {0}; mkdir -p /tmp/lithops; '.format(REMOTE_INSTALL_DIR)
        ep_vsi_data = {'ip_address': ip_address, 'instance_id': ep_instance.get_instance_id()}
        cmd += "echo '{}' > {}/access.data; ".format(json.dumps(ep_vsi_data), REMOTE_INSTALL_DIR)
        cmd += "echo '{}' > {}/config; ".format(json.dumps(self.config), REMOTE_INSTALL_DIR)
        cmd += "mv /tmp/*.py '{}'; ".format(REMOTE_INSTALL_DIR)
        cmd += "apt-get install python3-paramiko -y; ".format(REMOTE_INSTALL_DIR)
        # Run setup script
        cmd += 'python3 /opt/lithops/setup.py; >> /tmp/lithops/proxy.log 2>&1; '

        logger.debug('Executing lithops installation process trough entry point VSI {}'.format(ip_address))
        logger.debug('Be patient, initial installation process could take up to 5 minutes')
        ssh_client.run_remote_command(cmd)
        logger.debug('Completed lithops installation process trough entry point VSI {}'.format(ip_address))
