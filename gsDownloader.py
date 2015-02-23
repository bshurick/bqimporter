import httplib2
from apiclient.discovery import build
from apiclient.http import MediaIoBaseDownload
from apiclient.errors import HttpError
from oauth2client.client import SignedJwtAssertionCredentials
from json import dumps
from hashlib import sha256
import logging
from datetime import datetime
logging.basicConfig()
logger = logging.getLogger(__name__)

# Retry transport and file IO errors.
RETRYABLE_ERRORS = (httplib2.HttpLib2Error, IOError)

# Download chunk size 
CHUNKSIZE = 2 * 1024 * 1024

class GsDownloader:
	def __init__(self, service_account_email, key):
		''' Pass account email and project number to initialize '''
		self._service_account_email = service_account_email
		self._key = key
		self._active_jobs = []
		self._credentials = SignedJwtAssertionCredentials(
			   self._service_account_email,
			   self._key,
			   scope='https://www.googleapis.com/auth/devstorage.read_write'
	    )
		self._service = build('storage', 'v1')
	
	def _authenticate_service(self):
		''' Authorize credentials '''
		http = self._credentials.authorize(httplib2.Http())
		self._service = build('storage', 'v1', http=http)
	
	def _authorize(self):
		'''Authorize credentials'''
		http = httplib2.Http()
		return self._credentials.authorize(http)
	
	def handle_progressless_iter(error, progressless_iters):
		''' Progressess iteration function 
			Adapted from https://code.google.com/p/google-cloud-platform-samples/source/browse/file-transfer-json/chunked_transfer.py?repo=storage
		'''
		if progressless_iters > NUM_RETRIES:
			logger.warning('Failed to make progress for too many consecutive iterations.')
			raise error
		
		sleeptime = random.random() * (2**progressless_iters)
		logger.warning('Caught exception (%s). Sleeping for %s seconds before retry #%d.'
			% (str(error), sleeptime, progressless_iters))
		time.sleep(sleeptime)
	
	def download(self, bucket, object, destination):
		''' File download in chunks
			Adapted from https://code.google.com/p/google-cloud-platform-samples/source/browse/file-transfer-json/chunked_transfer.py?repo=storage
		'''
		self._authenticate_service()
		f = file(destination,'w')
		request = self._service.objects().get_media(
				bucket=bucket
				,object=object
		)
		media = MediaIoBaseDownload(f, request, chunksize=CHUNKSIZE)
		logger.warning('Downloading bucket {0} to file {1}'.format(bucket,object))
		progressless_iters = 0
		done = False
		while not done:
			error = None
			try:
				progress, done = media.next_chunk()
				if progress:
					logger.warning(
						'Download %d%%.' % int(progress.progress() * 100)
					)
			except HttpError, err:
				error = err
				if err.resp.status < 500:
					raise
			except RETRYABLE_ERRORS, err:
				error = err
			
			if error:
				progressless_iters += 1
				handle_progressless_iter(error, progressless_iters)
			else:
				progressless_iters = 0
		logger.warning('Download complete!')
	
	def delete_obj(self, bucket, object):
		''' Remove an object from a bucket '''
		self._authenticate_service()
		request = self._service.objects().delete(
			bucket=bucket,
			object=object
		).execute() 
