from bigquery import get_client
import httplib2
from apiclient.discovery import build
from oauth2client.client import SignedJwtAssertionCredentials
from json import dumps
import logging
from datetime import datetime
logging.basicConfig()
logger = logging.getLogger(__name__)

class bqImporter:
	def __init__(self, service_account_email, project_number, key):
		''' Pass account email and project number to initialize '''
		self._service_account_email = service_account_email
		self._project_number = project_number
		self._key = key
		self._active_jobs = []
		self._service = build('bigquery', 'v2')
		self._client = get_client(self._project_number, service_account=self._service_account_email,
                    private_key=self._key, readonly=True)
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
		return self._return_self().strip()

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
	
	def run_query(self, query_str):
		''' Execute query using bigquery-python and wait for results
			Args: 
				query_str: a BigQuery query string 
		''' 
		job_id, _results = self._client.query(query_str)
		logger.warning('Started job_id: '+job_id)
		self._active_jobs.append(job_id)
		return (job_id, _results)
	
	def job_isfinished(self,job_id):
		''' Check to see if complete status is true '''
		complete, row_count = self._client.check_job(job_id)
		return complete
	
	def get_query_results(self,job_id):
		''' Execute query using bigquery-python and wait for results
			Args: 
				job_id: a BigQuery job ID
		''' 
		if job_id not in self._active_jobs: raise Exception ('Job not active')
		else: 
			self._active_jobs.remove(job_id)
			return self._client.get_query_rows(job_id)
	
	def get_tabledef(self, datasetId, tableId):
		'''Print table schema for a given dataset and table'''
		http = self._authorize()
		tables = self._service.tables()
		response = tables.get(projectId=self._project_number,datasetId=datasetId,tableId=tableId).execute(http=http)
		return response
	
	def get_datasets(self):
		'''Return a list of all datasets for active client'''
		response = self._client.get_datasets()
		return [ '{0}'.format(d['datasetReference']['datasetId']) for d in response ]
	
	def get_tables(self, datasetId):
		'''Print all table IDs for a given dataset'''
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
