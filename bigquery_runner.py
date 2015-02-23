from bqImporter import BqImporter
from gsDownloader import GsDownloader
from copy_into_vertica import VerticaCopyRunner
from re import match 
import logging, os, imp
from datetime import datetime
from dateutil.relativedelta import relativedelta
import ConfigParser
from optparse import OptionParser

# Enable logging
logging.basicConfig()
logger = logging.getLogger(__name__)

# Set config and command-line parser options
parser = OptionParser()
config = ConfigParser.RawConfigParser()
parser.add_option('-c','--config',action='store',type='string',dest='config',default='config.ini',help='Read from config file',metavar='config.ini')
parser.add_option('-s','--startdt',action='store',type='string',dest='startdt',default=str(datetime.date(datetime.now()-relativedelta(days=1))),help='Set the starting date',metavar='YYYY-MM-DD')
parser.add_option('-e','--enddt',action='store',type='string',dest='enddt',default=str(datetime.date(datetime.now())),help='Set the ending date',metavar='YYYY-MM-DD')
parser.add_option('-x',action='store_true',dest='drop',default=False,help='Drop the destination table in BigQuery before running (if schema changes)')
parser.add_option('-v',action='store_true',dest='vertica',default=False,help='Import into Vertica')
parser.add_option('-t',action='store_true',dest='truncate',default=False,help='Truncate Vertica table before loading')
parser.add_option('-d',action='store_true',dest='dedupe',default=False,help='Dedupe Vertica table after loading')

class BigQueryRunner:
	''' Class to combine all functions into a script that will execute queries in 
		BiqQuery using predefined query templates and dataset keys, with 
		a schema defined for the flattened output
	'''
	def __init__(self,dataset,table,startdt,enddt,schema,table_match_re,template,final_step,dataset_keys,project_number,service_account_email,key,uris,drop_before=False):
		''' Initialize query runner object
			Args: 
				dataset: destination datasetId (should already exist)
				table: destination tableId (will be created if necessary)
				startdt: start date, formatted 'YYYY-MM-DD'
				enddt: end date, formatted 'YYYY-MM-DD'
				schema: schema for table in BigQuery to be used / created
				table_match_re: regular expression to match table name and extract 8-digit 'datenum'
				template: template for query that will run for each WW dataset and table
				final_step: final select statement that will aggregate all data together from template
				dataset_keys: dict of WW datasets (i.e. with country codes)
				project_number: Google account project number, for login
				service_account_email: Google service account email, for login
				key: p12 key from Google, for login
				uris: Google storage URIs (for transferring query results - not optional)
				drop_before: boolean to drop table prior to running query (in case of schema change, set to true)
		'''
		self.null_val = lambda x: None if str(x).upper()=='(NOT SET)' else x
		self.startdt = startdt
		self.enddt = enddt
		self.dts = self.get_dates(startdt,enddt)
		self.destination_dataset = dataset 
		self.destination_table = table
		self.uris = uris
		self._project_number = project_number
		self._service_account_email = service_account_email
		self.bq = BqImporter(self._service_account_email, self._project_number, key)
		self.dataset_keys = dataset_keys
		self.table_schema = schema
		self.table_match_re = table_match_re
		self.template = template
		self.final_step = final_step
		self.drop_before = drop_before
	
	def _return_self(self):
		return '''
			Start Date: {sdt}
			End Date: {edt}
			Destination table: {dd}.{dt}
			Query Template: \n{templ}
		'''.format(sdt=self.startdt,edt=self.enddt,dd=self.destination_dataset,dt=self.destination_table,templ=self.template)
	
	def __repr__(self):	
		return self._return_self()
	
	def __str__(self):
		return self._return_self()
	
	def table_match(self,match_str, table_match_re):
		''' Returns True if table name matches naming convention and dates match dts argument 
			Regex requires a 'datenum' group with YYYYMMDD-formatted date number
		'''
		try:
			d = match(table_match_re,match_str).group('datenum')
		except:
			return False
		return d[:4]+'-'+d[4:6]+'-'+d[6:8] in self.dts
	
	def create_table_list(self):
		''' Create list of formatted, comma-separated tables to union within query statement 
			Query for each table will be based on template string 
			Tables will be based on the dts parameter 
			Query template needs to match schema parameter 
			Final query will select all fields from union'd tables
		'''
		tables = ','.join([ m for m in 
			map(lambda x: ','.join(filter((lambda z: len(z)>0),x)), [
				map(
					lambda x: self.template.format(d,x,self.dataset_keys[d])
					,[z for z in t if self.table_match(z, self.table_match_re) ]
				) 
					for d,t in [ 
						(d,self.bq.get_tables(d)) 
						for d in self.dataset_keys 
					] 
				])
			if len(m)>0 
		])
		return tables
	
	def exec_query_wait(self,query_str):
		''' Execute query and wait for it to finish '''
		import time
		job = self.bq.query_to_table(query_str,self.destination_dataset,self.destination_table)
		while True:
			if self.bq.job_isfinished(job): break
			time.sleep(5)
		return True
	
	def setup_table(self):
		''' Check if table is already created and if not create it '''
		status = self.bq.check_table(self.destination_dataset,self.destination_table)
		if self.drop_before: 
			if status:
				self.bq.drop_table(self.destination_dataset,self.destination_table)
				logger.warning('Dropped table {0}.{1}'.format(self.destination_dataset,self.destination_table))
		if not status:
			try:
				created = self.bq.create_table('Test','bi_temp',self.table_schema)
			except Exception as e:
				raise e 
			return 'Created'
		else:
			return 'Exists'
	
	def get_dates(self,startdt,enddt):
		''' Return a list of date strings for parsing tables '''
		matchdt = lambda x: match(r'(?P<year>\d{4})([-]{1})(?P<month>\d{2})([-]{1})(?P<day>\d{2})',x)
		parsedt = lambda x: datetime.date(datetime(int(x.group('year')),int(x.group('month')),int(x.group('day'))))
		ms,me = matchdt(startdt),matchdt(enddt)
		sdt,edt = parsedt(ms),parsedt(me)
		dts = []
		while sdt<=edt:
			dts.append(str(sdt))
			sdt += relativedelta(days=1)
		return dts
	
	def run(self):
		''' Run big query runner 
			Create table if necessary 
			Create query, formatted based on template string 
			Execute query and wait for query to finish 
		'''	
		table_status = self.setup_table()
		logger.warning('Table '+table_status.lower())
		q = self.final_step.format(self.create_table_list())
		logger.warning('Query created successfully')
		logger.warning('Executing query from {0} to {1}...'.format(self.startdt,self.enddt))
		successful = self.exec_query_wait(q)
		if successful: logger.warning('Query successful!')
		logger.warning('Exporting data to cloud storage')
		if len(self.uris)>0: 
			job = self.bq.export_data_to_uris(
				self.uris 
				,self.destination_dataset
				,self.destination_table
				,compression='GZIP'
				,destination_format='CSV'
			)
			job_resource = self.bq.wait_for_job(job,timeout=300)
			logger.warning('Data exported to Cloud Storage at '+str(self.uris))

def main(parse_args,config):
	''' Run big query process and load data into Vertica '''
	
	# Grab configuration elements
	(options, args) = parse_args.parse_args()
	config.read(options.config)
	t = imp.load_source('query_template',config.get('BigQuery','template_file'))
	PROJECT_NUMBER = config.get('Connection','Project_Number')
	SERVICE_ACCOUNT_EMAIL = config.get('Connection','Service_Account_Email')
	GS_BUCKET = config.get('GoogleStorage','gs_bucket')
	GS_OBJ = config.get('GoogleStorage','gs_dest_object')
	DEST_FILE = config.get('Destination','destination_file')
	VERTICA_TABLE = config.get('Destination','vertica_table')
	BQ_TABLE = config.get('BigQuery','bq_table')
	BQ_DATASET = config.get('BigQuery','bq_dataset')
	KEY = file(config.get('Connection','Key_File')).read()
	
	logger.warning('Starting Big Query Runner process')
	
	# Create a big query runner object and execute query
	# Results will be stored temporarily in Google Cloud Storage
	q = BigQueryRunner(
		dataset=BQ_DATASET 
		,table=BQ_TABLE
		,uris=['gs://{0}/{1}'.format(GS_BUCKET,GS_OBJ)]
		,startdt=options.startdt
		,enddt=options.enddt
		,schema=t.SCHEMA
		,table_match_re=t.TABLE_MATCH_RE
		,template=t.TEMPLATE
		,final_step=t.FINAL_SELECT
		,dataset_keys=t.DATASET_KEYS
		,project_number=PROJECT_NUMBER
		,service_account_email=SERVICE_ACCOUNT_EMAIL
		,key=KEY
		,drop_before=options.drop
	)
	q.run()
	
	if options.vertica:
		# Download data to temporary flat-file destination 
		# Remove data from Google Cloud Storage after download
		x = GsDownloader(SERVICE_ACCOUNT_EMAIL, KEY)
		x.download(GS_BUCKET, GS_OBJ, DEST_FILE)
		x.delete_obj(GS_BUCKET, GS_OBJ)
		
		# Copy data into Vertica
		# columns = [ x['name'] for x in q.SCHEMA ]
		c = VerticaCopyRunner(VERTICA_TABLE, t.COPY_COLUMNS, DEST_FILE, skip=1, delimiter=',', terminator=None, truncate=options.truncate, dedupe=options.dedupe)
		c.run()
		
		# Remove file 
		os.remove(DEST_FILE)
	
	logger.warning('Big Query Runner process completed')

def test_query(startdt,enddt):
	''' Create a test_query object for testing 
		Args:
			startdt: the start date of the query (YYYY-MM-DD)
			enddt: the end date of the query(YYYY-MM-DD)
	'''
	config = ConfigParser.RawConfigParser()
	config.read('config.ini')
	PROJECT_NUMBER = config.get('Connection','Project_Number')
	SERVICE_ACCOUNT_EMAIL = config.get('Connection','Service_Account_Email')
	KEY = file(config.get('Connection','Key_File')).read()
	t = imp.load_source('query_template',config.get('BigQuery','template_file'))
	q = BigQueryRunner(
		dataset='Test'
		,table='bi_temp'
		,uris=['gs://bi_bucket/test_data.csv.gz']
		,startdt=startdt
		,enddt=enddt 
		,schema=t.SCHEMA
		,table_match_re=t.TABLE_MATCH_RE
		,template=t.TEMPLATE
		,final_step=t.FINAL_SELECT
		,dataset_keys=t.DATASET_KEYS
		,project_number=PROJECT_NUMBER
		,service_account_email=SERVICE_ACCOUNT_EMAIL
		,key=KEY 
		,drop_before=True
	)
	return q

def test_downloader():
	''' Create a test_downloader object for testing 
	'''
	config = ConfigParser.RawConfigParser()
	config.read('config.ini')
	PROJECT_NUMBER = config.get('Connection','Project_Number')
	SERVICE_ACCOUNT_EMAIL = config.get('Connection','Service_Account_Email')
	GS_BUCKET = config.get('GoogleStorage','gs_bucket')
	GS_OBJ = config.get('GoogleStorage','gs_dest_object')
	DEST_FILE = config.get('Destination','destination_file')
	KEY = file(config.get('Connection','Key_File')).read()
	x = GsDownloader(SERVICE_ACCOUNT_EMAIL, KEY)
	return x, GS_BUCKET, GS_OBJ, DEST_FILE

if __name__=='__main__':
	main(parser,config)
