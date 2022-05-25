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
import logging
import json
import base64
import httplib2

import zipfile
import time

import lithops
from google.cloud import pubsub_v1
from google.oauth2 import service_account
from google_auth_httplib2 import AuthorizedHttp
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.auth import jwt

from lithops import utils
from lithops.version import __version__
from lithops.constants import COMPUTE_CLI_MSG, JOBS_PREFIX
from lithops.constants import TEMP as TEMP_PATH

from . import config

logger = logging.getLogger(__name__)

ZIP_LOCATION = os.path.join(TEMP_PATH, 'lithops_gcp_functions.zip')
SCOPES = ('https://www.googleapis.com/auth/cloud-platform',
          'https://www.googleapis.com/auth/pubsub')
FUNCTIONS_API_VERSION = 'v1'
PUBSUB_API_VERSION = 'v1'
AUDIENCE = "https://pubsub.googleapis.com/google.pubsub.v1.Publisher"


class GCPFunctionsBackend:
    def __init__(self, gcf_config, internal_storage):
        self.name = 'gcp_functions'
        self.type = 'faas'
        self.gcf_config = gcf_config

        self.region = gcf_config['region']
        self.service_account = gcf_config['service_account']
        self.project = gcf_config['project_name']
        self.credentials_path = gcf_config['credentials_path']
        self.num_retries = gcf_config['retries']
        self.retry_sleep = gcf_config['retry_sleep']
        self.trigger = gcf_config['trigger']

        self.internal_storage = internal_storage

        # Setup Pub/Sub client
        try:  # Get credentials from JSON file
            service_account_info = json.load(open(self.credentials_path))
            credentials = jwt.Credentials.from_service_account_info(
                service_account_info,
                audience=AUDIENCE
            )
        except Exception as e:  # Get credentials from gcp function environment
            credentials = None
        self.publisher_client = pubsub_v1.PublisherClient(credentials=credentials)

        msg = COMPUTE_CLI_MSG.format('Google Cloud Functions')
        logger.info(f"{msg} - Region: {self.region} - Project: {self.project}")

    def _format_function_name(self, runtime_name, runtime_memory):
        version = 'lithops_v' + __version__
        runtime_name = (version + '_' + runtime_name).replace('.', '-')
        return f'{runtime_name}_{runtime_memory}MB'
    
    def _unformat_function_name(self, function_name):
        version, runtime_name, runtime_memory = function_name.rsplit('_', 2)
        version = version.replace('lithops_v', '').replace('-', '.')
        return version, runtime_name, runtime_memory.replace('MB', '')

    def _format_topic_name(self, runtime_name, runtime_memory):
        return self._format_function_name(runtime_name, runtime_memory) +'_'+ self.region + '_topic'

    def _get_default_runtime_name(self):
        py_version = utils.CURRENT_PY_VERSION.replace('.', '')
        return  f'lithops-default-runtime-v{py_version}'

    def _full_function_location(self, function_name):
        return f'projects/{self.project}/locations/{self.region}/functions/{function_name}'

    def _full_topic_location(self, topic_name):
        return f'projects/{self.project}/topics/{topic_name}'

    def _full_default_location(self):
        return f'projects/{self.project}/locations/{self.region}'

    def _encode_payload(self, payload):
        return base64.b64encode(bytes(json.dumps(payload), 'utf-8')).decode('utf-8')

    def _get_auth_session(self):
        credentials = service_account.Credentials.from_service_account_file(self.credentials_path,
                                                                            scopes=SCOPES)
        http = httplib2.Http()
        return AuthorizedHttp(credentials, http=http)

    def _get_funct_conn(self):
        http = self._get_auth_session()
        return build('cloudfunctions', FUNCTIONS_API_VERSION, http=http, cache_discovery=False)

    def _get_runtime_requirements(self, runtime_name):
        if runtime_name == self._get_default_runtime_name():
            return config.DEFAULT_REQUIREMENTS
        else:
            user_runtimes = self._list_built_runtimes(default_runtimes=False)
            if runtime_name in user_runtimes:
                raw_reqs = self.internal_storage.get_data(key='/'.join([config.USER_RUNTIMES_PREFIX, runtime_name]))
                reqs = raw_reqs.decode('utf-8')
                return reqs.splitlines()
            else:
                available_runtims = [self._get_default_runtime_name()]+user_runtimes
                raise Exception(f'Runtime {runtime_name} does not exist. '
                    f'Available runtimes: {available_runtims}')

    def _list_built_runtimes(self, default_runtimes=True):
        runtimes = []

        if default_runtimes:
            runtimes.extend(self._get_default_runtime_name())

        user_runtimes_keys = self.internal_storage.storage.list_keys(
            self.internal_storage.bucket, prefix=config.USER_RUNTIMES_PREFIX
        )
        runtimes.extend([runtime.split('/', 1)[-1] for runtime in user_runtimes_keys])
        return runtimes

    def _create_handler_zip(self, runtime_name):
        logger.debug(f"Creating function handler zip in {ZIP_LOCATION}")

        def add_folder_to_zip(zip_file, full_dir_path, sub_dir=''):
            for file in os.listdir(full_dir_path):
                full_path = os.path.join(full_dir_path, file)
                if os.path.isfile(full_path):
                    zip_file.write(full_path, os.path.join('lithops', sub_dir, file), zipfile.ZIP_DEFLATED)
                elif os.path.isdir(full_path) and '__pycache__' not in full_path:
                    add_folder_to_zip(zip_file, full_path, os.path.join(sub_dir, file))

        # Get runtime requirements
        runtime_requirements = self._get_runtime_requirements(runtime_name)
        requirements_file_path = os.path.join(TEMP_PATH, f'{runtime_name}_requirements.txt')
        with open(requirements_file_path, 'w') as reqs_file:
            for req in runtime_requirements:
                reqs_file.write(f'{req}\n')

        try:
            with zipfile.ZipFile(ZIP_LOCATION, 'w') as lithops_zip:
                # Add Lithops entryfile to zip archive
                current_location = os.path.dirname(os.path.abspath(__file__))
                main_file = os.path.join(current_location, 'entry_point.py')
                lithops_zip.write(main_file, 'main.py', zipfile.ZIP_DEFLATED)

                # Add runtime requirements.txt to zip archive
                lithops_zip.write(requirements_file_path, 'requirements.txt', zipfile.ZIP_DEFLATED)

                # Add Lithops to zip archive
                module_location = os.path.dirname(os.path.abspath(lithops.__file__))
                add_folder_to_zip(lithops_zip, module_location)
        except Exception as e:
            raise Exception(f'Unable to create Lithops package: {e}')

    def _create_function(self, runtime_name, memory, timeout=60):
        """
        Creates all the resources needed by a function
        """
        # Create topic
        topic_name = self._format_topic_name(runtime_name, memory)
        topic_list_response = self.publisher_client.list_topics(
            request={'project': f'projects/{self.project}'})
        topic_location = self._full_topic_location(topic_name)
        topics = [topic.name for topic in topic_list_response]
        if topic_location in topics:
            logger.debug(f"Topic {topic_location} already exists - Restarting queue")
            self.publisher_client.delete_topic(topic=topic_location)
        logger.debug(f"Creating topic {topic_location}")
        self.publisher_client.create_topic(name=topic_location)

        # create the ZIP file
        default_location = self._full_default_location()
        function_location = self._full_function_location(self._format_function_name(runtime_name, memory))
        bin_name = self._format_function_name(runtime_name, memory) + '_bin.zip'
        try:
            self._create_handler_zip(runtime_name)
            with open(ZIP_LOCATION, "rb") as action_zip:
                action_bin = action_zip.read()
            self.internal_storage.put_data(bin_name, action_bin)
        finally:
            os.remove(ZIP_LOCATION)

        # Create the function
        fn_list_response = self._get_funct_conn().projects().locations().functions().list(
            parent=self._full_default_location()
        ).execute(num_retries=self.num_retries)
        if 'functions' in fn_list_response:
            deployed_functions = [fn['name'] for fn in fn_list_response['functions']]
            if function_location in deployed_functions:
                logger.debug(f"Function {function_location} already exists - Deleting function")
                self._get_funct_conn().projects().locations().functions().delete(
                    name=function_location,
                ).execute(num_retries=self.num_retries)


        cloud_function = {
            'name': function_location,
            'description': 'Lithops Worker for Lithops v'+ __version__,
            'entryPoint': 'main',
            'runtime': config.AVAILABLE_PY_RUNTIMES[utils.CURRENT_PY_VERSION],
            'timeout': str(timeout) + 's',
            'availableMemoryMb': memory,
            'serviceAccountEmail': self.service_account,
            'maxInstances': 0,
            'sourceArchiveUrl': f'gs://{self.internal_storage.bucket}/{bin_name}'
        }

        if self.trigger == 'http':
            cloud_function['httpsTrigger'] = {}

        elif self.trigger == 'pub/sub':
            topic_location = self._full_topic_location(self._format_topic_name(runtime_name, memory))
            cloud_function['eventTrigger'] = {
                'eventType': 'providers/cloud.pubsub/eventTypes/topic.publish',
                'resource': topic_location,
                'failurePolicy': {}
            }

        logger.debug(f'Creating function {function_location}')
        response = self._get_funct_conn().projects().locations().functions().create(
            location=default_location,
            body=cloud_function
        ).execute(num_retries=self.num_retries)

        # Wait until function is completely deployed
        logger.info('Waiting for the function to be deployed')
        while True:
            response = self._get_funct_conn().projects().locations().functions().get(
                name=function_location
            ).execute(num_retries=self.num_retries)
            if response['status'] == 'ACTIVE':
                break
            elif response['status'] == 'OFFLINE':
                raise Exception('Error while deploying Cloud Function')
            elif response['status'] == 'DEPLOY_IN_PROGRESS':
                time.sleep(self.retry_sleep)
                logger.info('...')
            else:
                raise Exception(f"Unknown status {response['status']}")


        # Delete runtime bin archive from storage
        self.internal_storage.storage.delete_object(self.internal_storage.bucket, bin_name)

    def build_runtime(self, runtime_name, requirements_file, extra_args=[]):
        logger.info(f'Building runtime {runtime_name} from {requirements_file}')

        if requirements_file is None:
            raise Exception('Please provide a `requirements.txt` file with the necessary modules')

        with open(requirements_file, 'r') as req_file:
            requirements = req_file.read()

        self.internal_storage.put_data('/'.join([config.USER_RUNTIMES_PREFIX, runtime_name]), requirements)
        logger.info(f'Runtime {runtime_name} created successfuly')


    def deploy_runtime(self, runtime_name, memory, timeout=60):
        logger.info(f"Deploying runtime: {runtime_name} - Memory: {memory} Timeout: {timeout}")

        self._create_function(runtime_name, memory, timeout=timeout)

        # Get runtime preinstalls
        runtime_meta = self._generate_runtime_meta(runtime_name, memory)

        return runtime_meta

    def delete_runtime(self, runtime_name, runtime_memory, delete_runtime_storage=True):
        action_name = self._format_function_name(runtime_name, runtime_memory)
        function_location = self._full_function_location(action_name)
        logger.debug(f'Going to delete runtime {action_name}')

        # Delete function
        self._get_funct_conn().projects().locations().functions().delete(
            name=function_location,
        ).execute(num_retries=self.num_retries)
        logger.debug('Request Ok - Waiting until function is completely deleted')

        # Wait until function is completely deleted
        while True:
            try:
                response = self._get_funct_conn().projects().locations().functions().get(
                    name=function_location
                ).execute(num_retries=self.num_retries)
                logger.debug(f'Function status is {response["status"]}')
                if response['status'] == 'DELETE_IN_PROGRESS':
                    time.sleep(self.retry_sleep)
                else:
                    raise Exception(f'Unknown status: {response["status"]}')
            except Exception as e:
                break

        # Delete Pub/Sub topic attached as trigger for the cloud function
        logger.debug('Listing Pub/Sub topics')
        topic_name = self._format_topic_name(runtime_name, runtime_memory)
        topic_location = self._full_topic_location(topic_name)
        topic_list_request = self.publisher_client.list_topics(
            request={f'project': 'projects/{self.project}'}
        )
        topics = [topic.name for topic in topic_list_request]
        if topic_location in topics:
            logger.debug(f'Going to delete topic {topic_name}')
            self.publisher_client.delete_topic(topic=topic_location)
            logger.debug(f'Ok - topic {topic_name} deleted')

        # Delete user runtime from storage
        user_runtimes = self._list_built_runtimes(default_runtimes=False)
        if runtime_name in user_runtimes and delete_runtime_storage:
            self.internal_storage.storage.delete_object(
                self.internal_storage.bucket, '/'.join([config.USER_RUNTIMES_PREFIX, runtime_name]))

    def clean(self):
        logger.debug('Going to delete all deployed runtimes')
        runtimes = self.list_runtimes()
        for runtime_name, runtime_memory, version in runtimes:
            self.delete_runtime(runtime_name, runtime_memory)

    def list_runtimes(self, runtime_name='all'):
        logger.debug('Listing deployed runtimes')
        response = self._get_funct_conn().projects().locations().functions().list(
            parent=self._full_default_location()
        ).execute(num_retries=self.num_retries)

        deployed_runtimes = [f['name'].split('/')[-1] for f in response.get('functions', []) if 'lithops_v' in f['name']]
        runtimes = []
        for func_runtime in deployed_runtimes:
            if 'lithops_v' in func_runtime:
                version, fn_name, memory = self._unformat_function_name(func_runtime)
                if runtime_name == fn_name or runtime_name == 'all':
                    runtimes.append((fn_name, memory, version))

        return runtimes

    def invoke(self, runtime_name, runtime_memory, payload={}):
        topic_location = self._full_topic_location(self._format_topic_name(runtime_name, runtime_memory))

        fut = self.publisher_client.publish(
            topic_location,
            bytes(json.dumps(payload, default=str).encode('utf-8'))
        )
        invocation_id = fut.result()

        return invocation_id

    def _generate_runtime_meta(self, runtime_name, memory):
        logger.debug(f'Generating runtime meta for {runtime_name}')

        function_name = self._format_function_name(runtime_name, memory)
        function_location = self._full_function_location(function_name)

        payload = {
            'get_preinstalls': {
                'runtime_name': runtime_name,
                'storage_config': self.internal_storage.storage.storage_config
            }
        }

        # Data is b64 encoded so we can treat REST call the same as async pub/sub event trigger
        response = self._get_funct_conn().projects().locations().functions().call(
            name=function_location,
            body={'data': json.dumps({'data': self._encode_payload(payload)})}
        ).execute(num_retries=self.num_retries)

        if 'result' in response and response['result'] == 'OK':
            object_key = '/'.join([JOBS_PREFIX, runtime_name + '.meta'])

            runtime_meta_json = self.internal_storage.get_data(object_key)
            runtime_meta = json.loads(runtime_meta_json)
            self.internal_storage.storage.delete_object(self.internal_storage.bucket, object_key)
            return runtime_meta
        elif 'error' in response:
            raise Exception(response['error'])
        else:
            raise Exception(f'Error at retrieving runtime meta: {response}')

    def get_runtime_key(self, runtime_name, runtime_memory):
        action_name = self._format_function_name(runtime_name, runtime_memory)
        runtime_key = os.path.join(self.name, self.region, action_name)
        logger.debug(f'Runtime key: {runtime_key}')
        return runtime_key

    def get_runtime_info(self):
        """
        Method that returns all the relevant information about the runtime set
        in config
        """
        if utils.CURRENT_PY_VERSION not in config.AVAILABLE_PY_RUNTIMES:
            raise Exception(f'Python {utils.CURRENT_PY_VERSION} is not available for Google '
             f'Cloud Functions. Please use one of {config.AVAILABLE_PY_RUNTIMES.keys()}')

        if 'runtime' not in self.gcf_config or self.gcf_config['runtime'] == 'default':
            self.gcf_config['runtime'] = self._get_default_runtime_name()
        
        runtime_info = {
            'runtime_name': self.gcf_config['runtime'],
            'runtime_memory': self.gcf_config['runtime_memory'],
            'runtime_timeout': self.gcf_config['runtime_timeout'],
            'max_workers': self.gcf_config['max_workers'],
        }

        return runtime_info
