#
# (C) Copyright Cloudlab URV 2024
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

from .gate import OneGateClient


import os
import pika
import hashlib
import json
import logging
import copy
import time

from lithops import utils
from lithops.version import __version__
from lithops.constants import COMPUTE_CLI_MSG, JOBS_PREFIX

logger = logging.getLogger(__name__)


class OpenNebulaBackend:
    """
    A wrap-up around OpenNebula backend.
    """

    def __init__(self, one_config, internal_storage):
        logger.info("Initializing OpenNebula backend")

        # Backend configuration
        self.name = 'one'
        self.type = utils.BackendType.BATCH.value
        self.one_config = one_config
        self.internal_storage = internal_storage
        self.amqp_url = self.one_config['amqp_url']

        # Init rabbitmq
        params = pika.URLParameters(self.amqp_url)
        self.connection = pika.BlockingConnection(params)
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue='task_queue', durable=True)

        # OpenNebula configuration
        logger.debug("Initializing OneGate python client")
        self.client = OneGateClient()

        msg = COMPUTE_CLI_MSG.format('OpenNebula')
        logger.info(f"{msg}")

    def _format_job_name(self, runtime_name, runtime_memory, version=__version__):
        name = f'{runtime_name}-{runtime_memory}-{version}'
        name_hash = hashlib.sha1(name.encode()).hexdigest()[:10]

        return f'lithops-worker-{version.replace(".", "")}-{name_hash}'

    def build_runtime(self, one_image_name, one_file, extra_args=[]):
        """
        Builds a new runtime from a Dockerfile file and pushes it to the registry
        """
        pass

    def _create_default_runtime(self):
        """
        Builds the default runtime
        """
        pass

    def deploy_runtime(self, one_image_name, memory, timeout):
        """
        Deploys a new runtime
        """
        logger.info(f"Deploying fake runtime: {one_image_name}")
        runtime_meta = self._generate_runtime_meta(one_image_name)

        return runtime_meta

    def delete_runtime(self, one_image_name, memory, version):
        """
        Deletes a runtime
        """
        pass

    def clean(self, all=False):
        """
        Deletes all jobs
        """
        # TODO: scale down
        logger.debug('Cleaning RabbitMQ queues')
        self.channel.queue_delete(queue='task_queue')

    def list_runtimes(self, one_image_name='all'):
        """
        List all the runtimes
        return: list of tuples (one_image_name, memory)
        """
        pass

    def invoke(self, one_image_name, runtime_memory, job_payload):
        """
        Invoke -- return information about this invocation
        For array jobs only remote_invocator is allowed
        """
        # TODO: scale up
        job_key = job_payload['job_key']
        granularity = self.one_config['worker_processes']
        times, res = divmod(job_payload['total_calls'], granularity)

        for i in range(times + (1 if res != 0 else 0)):
            num_tasks = granularity if i < times else res
            payload_edited = job_payload.copy()

            start_index = i * granularity
            end_index = start_index + num_tasks

            payload_edited['call_ids'] = payload_edited['call_ids'][start_index:end_index]
            payload_edited['data_byte_ranges'] = payload_edited['data_byte_ranges'][start_index:end_index]
            payload_edited['total_calls'] = num_tasks

            message = {
                'action': 'send_task',
                'payload': utils.dict_to_b64str(payload_edited)
            }

            self.channel.basic_publish(
                exchange='',
                routing_key='task_queue',
                body=json.dumps(message),
                properties=pika.BasicProperties(
                    delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
                ))

        activation_id = f'lithops-{job_key.lower()}'

        return activation_id

    def _generate_runtime_meta(self, one_image_name):
        runtime_name = self._format_job_name(one_image_name, 128)

        logger.info(f"Extracting metadata from: {one_image_name}")

        payload = copy.deepcopy(self.internal_storage.storage.config)
        payload['runtime_name'] = runtime_name
        payload['log_level'] = logger.getEffectiveLevel()
        encoded_payload = utils.dict_to_b64str(payload)

        message = {
            'action': 'get_metadata',
            'payload': encoded_payload
        }

        # Send message to RabbitMQ
        self.channel.basic_publish(
            exchange='',
            routing_key='task_queue',
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
            ))

        logger.debug("Waiting for runtime metadata")

        for i in range(0, 300):
            try:
                data_key = '/'.join([JOBS_PREFIX, runtime_name + '.meta'])
                json_str = self.internal_storage.get_data(key=data_key)
                runtime_meta = json.loads(json_str.decode("ascii"))
                self.internal_storage.del_data(key=data_key)
                break
            except Exception:
                time.sleep(2)

        if not runtime_meta or 'preinstalls' not in runtime_meta:
            raise Exception(f'Failed getting runtime metadata: {runtime_meta}')

        return runtime_meta

    def get_runtime_key(self, one_image_name, runtime_memory, version=__version__):
        """
        Method that creates and returns the runtime key.
        Runtime keys are used to uniquely identify runtimes within the storage,
        in order to know which runtimes are installed and which not.
        """
        jobdef_name = self._format_job_name(one_image_name, 256, version)
        runtime_key = os.path.join(self.name, version, jobdef_name)

        return runtime_key

    def get_runtime_info(self):
        """
        Method that returns all the relevant information about the runtime set
        in config
        """
        if 'runtime' not in self.one_config or self.one_config['runtime'] == 'default':
            py_version = utils.CURRENT_PY_VERSION.replace('.', '')
            self.one_config['runtime'] = f'one-runtime-v{py_version}'

        runtime_info = {
            'runtime_name': self.one_config['runtime'],
            'runtime_memory': self.one_config['runtime_memory'],
            'runtime_timeout': self.one_config['runtime_timeout'],
            'max_workers': self.one_config['max_workers'],
        }

        return runtime_info
