TABLE_MATCH_RE = r'(''web_sessions_'')(?P<datenum>\d{8})'
SCHEMA = [
	{'name': 'Col1', 'type': 'STRING', 'mode': 'nullable'},
	{'name': 'Col2', 'type': 'TIMESTAMP', 'mode': 'nullable'},
	{'name': 'Col3', 'type': 'INTEGER', 'mode': 'nullable'},
]
TEMPLATE = '''
	(
		SELECT 
		'{2}' as dataset_id 
		,hits.customDimensions.value
		,visitStartTime
		,date
		,totals.hits
		,totals.pageviews
		,totals.timeOnSite
		FROM [{0}.{1}]
		where (hits.customDimensions.index = 1 and hits.customDimensions.value != '')
		GROUP EACH BY 1,2,3,4,5,6,7
	)'''
FINAL_SELECT = '''
	SELECT * FROM {0} ;
'''
COPY_COLUMNS = '''
	dataset_id
	,customValue
	,unix_time FILLER varchar(15)
	,visitStartTime AS TO_TIMESTAMP(unix_time)
	,date
	,totalshits
	,totalspageviews
	,totalstimeOnSite
'''
DATASET_KEYS = {
	'00000000':'ID1',
	'11111111':'ID2',
}
