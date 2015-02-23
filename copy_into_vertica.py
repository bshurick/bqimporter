
from optparse import OptionParser
import subprocess, imp, logging, sys

# Read command-line params 
parser = OptionParser()
parser.add_option('-t','--table',action='store',type='string',dest='table',help='Destination Table',metavar='schema.table')
parser.add_option('-c','--columns',action='store',type='string',dest='columns',help='Comma-separated list of columns',metavar='Col1,Col2,Col3')
parser.add_option('-f','--file',action='store',type='string',dest='file',help='File to read data from',metavar='/file/loc')
parser.add_option('-s','--skip',action='store',type='int',dest='skip',help='Lines to skip (default 0)',metavar='0')
parser.add_option('-e','--enclosure',action='store',type='string',dest='enclosure',help='Enclosure (default ")',metavar='\'\\"\'')
parser.add_option('-d','--delimiter',action='store',type='string',dest='delimiter',help='Delimiter (default ;)',metavar='\';\'')
parser.add_option('-x','--terminator',action='store',type='string',dest='terminator',help='Line terminator (default E\'\\r\')',metavar='E\'\\r\'')
parser.add_option('-g',action='store_true',dest='gzip',default=False,help='Read Gzip file')
parser.add_option('-r',action='store_true',dest='truncate',default=False,help='Truncate table before copy statement')
parser.add_option('-u',action='store_true',dest='dedupe',default=False,help='De-dupe data after copy')

# Read global config file
CONFIG_FILE = '../../global.context.properties'
with open(CONFIG_FILE,'r') as r: properties = [ line.strip() for line in r if line[0]!='#' and line[0]!='[' and len(line.strip())>0 ]
CONFIG = {k: v for (k,v) in [ (z.split('=')[0],z.split('=')[1]) for z in properties ]}

# Enable logging
logging.basicConfig()
logger = logging.getLogger(__name__)

class VerticaCopyRunner:
	''' Simple class to instantiate a VSQL string and copy a data file into Vertica '''
	def __init__(self, table, columns, file, skip=0, enclosure = '\\"', delimiter = ';', terminator = '''E'\\r' ''', gzip=True, truncate=False, dedupe=False):
		''' Initialize configuration parameters 
			Args:
				table: name of destination table (must be pre-created)
				columns: list of column strings or manual comma-separated column definitions
				file: file to read data from 
				skip: (default 0) lines to skip 
				enclosure: (default ") string enclosure 
				delimiter: (default ;) file delimiter 
				terminator: (default E'\\r') end of line terminator 
				gzip: (default True) gzip compression on file
				truncate: (default False) truncate table before running
				dedupe: (default False) run a de-dupe process after copy
		'''
		self._server = CONFIG.get('vdwh_Server')
		self._login = CONFIG.get('vdwh_Login')
		self._pw = CONFIG.get('vdwh_Password')
		self._db = CONFIG.get('vdwh_Database')
		self.table = table 
		self.columns = columns
		self.file = file
		self.skip = skip
		self.enclosure = enclosure
		self.delimiter = delimiter
		self.terminator = 'RECORD TERMINATOR '+terminator if terminator else ''
		self.gzip = ' GZIP' if gzip else ''
		self.truncate = truncate
		self.dedupe = dedupe
	
	def _return_self(self):
		return '''
			Destination table: {0}
			Source file: {1}
		'''.format(self.table,self.file)
	
	def __repr__(self):
		return self._return_self()
	
	def __repr__(self):
		return self._return_self()
	
	def create_vsql_statement(self):
		return 'vsql -h{0} -d{1} -U{2} -w{3}'.format(self._server,self._db,self._login,self._pw)
	
	def create_copy_statement(self):
		''' Generate copy statement '''
		copy_statement = ''' {truncate}
		COPY {table} (
			{column_str}
		) 
		FROM LOCAL '{file}'{gzip} DIRECT SKIP {skip} EXCEPTIONS '{file}.exc' REJECTED DATA '{file}.rej' ENCLOSED BY '{enclosure}' DELIMITER '{delimiter}' {terminator} ;
		'''.format(
			table = self.table 
			,column_str = ','.join(self.columns) if isinstance(self.columns,list) else self.columns
			,skip = self.skip 
			,file = self.file 
			,enclosure = self.enclosure
			,delimiter = self.delimiter 
			,terminator = self.terminator
			,gzip = self.gzip
			,truncate = 'TRUNCATE TABLE {0};'.format(self.table) if self.truncate else ''
		)
		return copy_statement
	
	def execute_query(self, vsql, sql):
		active = [subprocess.Popen(vsql+' -c \"{0}\"'.format(sql),stdout=subprocess.PIPE,stderr=subprocess.STDOUT,shell=True)]
		while active:
			for p in active:
				if p.poll() is not None:
					std = [ line.strip() for line in p.stdout ]
					if std!='\n': logger.warning(std)
					p.stdout.close()
					active.remove(p)
	
	def run(self):
		logger.warning('Creating statement to copy into Vertica')
		vsql = self.create_vsql_statement()
		sql = self.create_copy_statement()
		logger.warning('Copying into Vertica with statement {0}'.format(sql))
		self.execute_query(vsql,sql)
		logger.warning('Completed copy into Vertica')
		if self.dedupe:
			logger.warning('Running dedupe process')
			dedupe_sql = ''' 
				DROP TABLE IF EXISTS X; 
				CREATE LOCAL TEMPORARY TABLE X ON COMMIT PRESERVE ROWS AS 
				SELECT DISTINCT * FROM {0};
				TRUNCATE TABLE {0};
				INSERT INTO {0} SELECT * FROM X; COMMIT;
			'''.format(self.table)
			self.execute_query(vsql,dedupe_sql)
			logger.warning('Dedupe finished')

def main(parser):
	(options, args) = parser.parse_args()
	try:
		c = VerticaCopyRunner(options.table, options.columns.split(','), options.file, options.skip, options.enclosure, options.delimiter, options.terminator, options.gzip, options.truncate, options.dedupe)
	except Exception as e:
		logger.error('Unable to parse arguments')
		sys.exit(1)
	c.run()

if __name__=='__main__':
	main(parser)
