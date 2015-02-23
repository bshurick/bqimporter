# from bigquery import get_client
import httplib2
from apiclient.discovery import build
from oauth2client.client import SignedJwtAssertionCredentials
from apiclient.errors import HttpError
from json import dumps
from hashlib import sha256
import logging
from datetime import datetime
from time import sleep,time
logging.basicConfig()
logger = logging.getLogger(__name__)

class BqImporter:
	def __init__(self, service_account_email, project_number, key):
		''' Pass account email and project number to initialize '''
		self._service_account_email = service_account_email
		self._project_number = project_number
		self._key = key
		self._active_jobs = []
		self._service = build('bigquery', 'v2')
		# self._client = get_client(self._project_number, service_account=self._service_account_email,
                    # private_key=self._key, readonly=True)
		self._credentials = SignedJwtAssertionCredentials(
			   self._service_account_email,
			   self._key,
			   scope='https://www.googleapis.com/auth/bigquery'
	    )
		self.cache = {}
	
	def _return_self(self):
		''' Print string representation of settings '''
		s= 'Email: {0}\nProject Number: {1}\n'.format(self._service_account_email,self._project_number)
		if self._active_jobs != []: 
			s+='Jobs Active: \n\t'
			s+='\n\t'.join(['Job_id: '+a for a in self._active_jobs])
		return s
	
	def __str__(self):
		''' Print string representation of settings and running jobs'''
		return self._return_self()

	def __repr__(self):
		''' Return string representation of settings and running jobs'''
		return self._return_self()
	
	def _authorize(self):
		'''Authorize credentials'''
		http = httplib2.Http()
		return self._credentials.authorize(http)
	
	def _check_jobs_active(self):
		''' Check to see if jobs have not been cleared '''
		return len(self._active_jobs)>0 
	
	# def run_query(self, query_str):
		# ''' Execute query using bigquery-python and wait for results
			# Args: 
				# query_str: a BigQuery query string 
		# ''' 
		# job_id, _results = self._client.query(query_str)
		# if len(_results)==0:
			# logger.warning('Started job_id: '+job_id)
			# self._active_jobs.append(job_id)
		# return (job_id, _results)
	
	def job_isfinished(self,job_id):
		''' Check to see if complete status is true '''
		http = self._authorize()
		jobs_collection = self._service.jobs()
		results = jobs_collection.getQueryResults(
			projectId=self._project_number
			,jobId=job_id
			,startIndex=0
			,maxResults=0
			,timeoutMs=0
		).execute(http=http)
		complete = results.get('jobComplete',False)
		return complete
	
	# def get_query_results(self,job_id):
		# ''' Execute query using bigquery-python and wait for results
			# Args: 
				# job_id: a BigQuery job ID
		# ''' 
		# if job_id not in self._active_jobs: raise Exception ('Job not active')
		# else: 
			# self._active_jobs.remove(job_id)
			# return self._client.get_query_rows(job_id)
	
	def get_tabledef(self, datasetId, tableId):
		'''Print table schema for a given dataset and table'''
		http = self._authorize()
		tables = self._service.tables()
		response = tables.get(projectId=self._project_number,datasetId=datasetId,tableId=tableId).execute(http=http)
		return response
	
	def get_datasets(self):
		'''Return a list of all datasets for active client'''
		http = self._authorize()
		response = self._service.datasets().list(projectId=self._project_number).execute(http=http)
		results = response.get('datasets',[])
		return [ '{0}'.format(d['datasetReference']['datasetId']) for d in results ]
	
	def get_table(self, dataset, table):
		''' Retrieve a table if it exists '''
		http = self._authorize()
		try:
			return self._service.tables().get(
				projectId=self._project_number
				,datasetId=dataset
				,tableId=table
			).execute(http=http)
		except HttpError, e:
			if int(e.resp['status']) == 404:
				logging.warn('Table %s.%s does not exist', dataset, table)
				return None
			raise
	
	def check_table(self, dataset, table):
		''' Check to see if a table exists '''
		table = self.get_table(dataset, table)
		return bool(table)
	
	def get_tables(self, datasetId):
		'''Print all table IDs for a given dataset
		Adapted from https://github.com/tylertreat/BigQuery-Python
		'''
		http = self._authorize()
		result = self._service.tables().list(
			projectId=self._project_number,
			datasetId=datasetId).execute(http=http)
		page_token = result.get('nextPageToken')
		while page_token:
			res = self._service.tables().list(
				projectId=self._project_number,
				datasetId=datasetId,
				pageToken=page_token
			).execute(http=http)
			page_token = res.get('nextPageToken')
			result['tables'] += res.get('tables', [])
		self.cache[datasetId] = (datetime.now(), result)
		return [ '{0}'.format(t['tableReference']['tableId']) for t in result['tables'] ]
	
	def list_jobs(self):
		http = self._authorize()
		jobs = self._service.jobs().list(
			projectId=self._project_number
		).execute(http=http)
		# page_token = jobs.get('nextPageToken')
		# while page_token:
			# res = self._service.jobs().list(
				# projectId=self._project_number
			# ).execute(http=http)
			# page_token = res.get('nextPageToken')
			# jobs['jobs'] += res.get('jobs',[])
		return jobs
	
	def _raise_executing_exception_if_error(self, job):
		''' Extract error message from job 
			Adapted from https://github.com/tylertreat/BigQuery-Python
		'''
		error_http = job.get('error')
		if error_http:
			raise Exception(
				"Error in export job API request: {0}".format(error_http))
		# handle errorResult - API request is successful but error in result
		error_result = job.get('status').get('errorResult')
		if error_result:
			raise Exception(
				"Reason:{reason}. Message:{message}".format(**error_result))
	
	def wait_for_job(self, job, interval=5, timeout=60):
		''' Wait until the job is done or failed 
			Adapted from https://github.com/tylertreat/BigQuery-Python
			Args:
				job: dict, representing a BigQuery job resource
				interval: optional float polling interval in seconds, default = 5
				timeout: optional float timeout in seconds, default = 60
		'''
		http = self._authorize()
		complete = False
		job_id = job['jobReference']['jobId']
		job_resource = None
		
		start_time = time()
		elapsed_time = 0
		while not (complete or elapsed_time > timeout):
			sleep(interval)
			request = self._service.jobs().get(projectId=self._project_number,
				jobId=job_id)
			job_resource = request.execute(http=http)
			self._raise_executing_exception_if_error(job_resource)
			complete = job_resource.get('status').get('state') == u'DONE'
			elapsed_time = time() - start_time
		
		# raise exceptions if timeout
		if not complete:
			logging.error('BigQuery job %s timeout' % job_id)
			raise BigQueryTimeoutException()
		
		return job_resource
	
	def get_jobinfo(self,job_id):
		http = self._authorize()
		job = self._service.jobs().get(
			projectId=self._project_number
			,jobId=job_id
		).execute(http=http)
		return job
	
	def create_table(self, datasetId, tableId, schema):
		"""Create a new table in the dataset.
		Adapted from https://github.com/tylertreat/BigQuery-Python
        Args:
            dataset: the dataset to create the table in.
            table: the name of table to create.
            schema: table schema dict.
        Returns:
            bool indicating if the table was successfully created or not,
            or response from BigQuery if swallow_results is set for False.
        """
		http = self._authorize()
		body = {
            'schema': {'fields': schema},
            'tableReference': {
                'tableId': tableId,
                'projectId': self._project_number,
                'datasetId': datasetId
            }
        }
		table = self._service.tables().insert(
			projectId=self._project_number,
			datasetId=datasetId,
			body=body
		).execute(http=http)
		return table
	
	def drop_table(self, datasetId, tableId):
		"""Delete a table from the dataset.
		Adapted from https://github.com/tylertreat/BigQuery-Python
		Args:
			datasetId: the dataset to delete the table from.
			tableId: the name of the table to delete.
		Returns:
			bool indicating if the table was successfully deleted or not,
			or response from BigQuery if swallow_results is set for False.
		"""
		http = self._authorize()
		response = self._service.tables().delete(
			projectId=self._project_number,
			datasetId=datasetId,
			tableId=tableId
		).execute(http=http)
		return response
	
	def query_to_table(self, query, datasetId, tableId, allow_large_results=True, show_job=False):
		""" Write query result to table. 
		Adapted from https://github.com/tylertreat/BigQuery-Python
		Args: 
			query: required BigQuery query string.
            dataset: required string id of the dataset
            table: required string id of the table
			allow_large_results: optional boolean
		"""
		http = self._authorize()
		configuration = {
            "query": query,
        }
		configuration['destinationTable'] = {
			"projectId": self._project_number,
			"tableId": tableId,
			"datasetId": datasetId
		}
		configuration['createDisposition'] = 'CREATE_IF_NEEDED'
		configuration['writeDisposition'] = 'WRITE_TRUNCATE'
		configuration['allowLargeResults'] = allow_large_results
		configuration['priority'] = 'BATCH'
		body = {
            "configuration": {
                'query': configuration
            }
        }
		if show_job: logger.warning('Writing to table {0}.{1} with job {2}'.format(datasetId,tableId,body))
		else: logger.warning('Writing to table {0}.{1}'.format(datasetId,tableId))
		job_resource = self._service.jobs().insert(
				projectId=self._project_number
				, body=body
			).execute(http=http)
		job_id = job_resource['jobReference']['jobId']
		self._active_jobs.append(job_id)
		return job_id
	
	def _generate_hex_for_uris(self, uris):
		"""Given uris, generate and return hex version of it
		Args:
			uris: A list containing all uris
		Returns:
			string of hexed uris
		"""
		return sha256(":".join(uris) + str(time())).hexdigest()
	
	def export_data_to_uris(
		self,
		destination_uris,
		dataset,
		table,
		job=None,
		compression=None,
		destination_format=None,
		print_header=None,
		field_delimiter=None,
	):
		"""
		Export data from a BigQuery table to cloud storage.
		Adapted from https://github.com/tylertreat/BigQuery-Python
		Args:
			destination_uris: required string or list of strings representing
				the uris on cloud storage of the form:
					gs://bucket/filename
			dataset: required string id of the dataset
			table: required string id of the table
			job: optional string identifying the job (a unique jobid
				is automatically generated if not provided)
			compression: optional string
				(one of the JOB_COMPRESSION_* constants)
			destination_format: optional string
				(one of the JOB_DESTINATION_FORMAT_* constants)
			print_header: optional boolean
			field_delimiter: optional string
			Optional arguments with value None are determined by
			BigQuery as described:
			https://developers.google.com/bigquery/docs/reference/v2/jobs
		Returns:
			dict, a BigQuery job resource
		Raises:
			JobInsertException on http/auth failures or error in result
		"""
		http = self._authorize()
		destination_uris = destination_uris \
			if isinstance(destination_uris, list) else [destination_uris]
		
		configuration = {
			"sourceTable": {
				"projectId": self._project_number,
				"tableId": table,
				"datasetId": dataset
			},
			"destinationUris": destination_uris,
		}
		
		if compression: configuration['compression'] = compression
		
		if destination_format: configuration['destinationFormat'] = destination_format
		
		if print_header is not None: configuration['printHeader'] = print_header
		
		if field_delimiter: configuration['fieldDelimiter'] = field_delimiter
		
		if not job:
			hex = self._generate_hex_for_uris(destination_uris)
			job = "{dataset}-{table}-{digest}".format(
				dataset=dataset,
				table=table,
				digest=hex
			)
		
		body = {
			"configuration": {
				'extract': configuration
			},
			"jobReference": {
				"projectId": self._project_number,
				"jobId": job
			}
		}
		
		logger.warning("Creating export job %s" % body)
		job_resource = self._service.jobs().insert(
			projectId=self._project_number
			, body=body
		).execute(http=http)
		return job_resource
