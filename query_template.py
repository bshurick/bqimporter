TABLE_MATCH_RE = r'(''ga_sessions_'')(?P<datenum>\d{8})'
SCHEMA = [
	{'name': 'origin', 'type': 'STRING', 'mode': 'nullable'},
	{'name': 'local_cid', 'type': 'STRING', 'mode': 'nullable'},
	{'name': 'visitNumber', 'type': 'INTEGER', 'mode': 'nullable'},
	{'name': 'visitStartTime', 'type': 'TIMESTAMP', 'mode': 'nullable'},
	{'name': 'date', 'type': 'INTEGER', 'mode': 'nullable'},
	{'name': 'totalsvisits', 'type': 'INTEGER', 'mode': 'nullable'},
	{'name': 'totalshits', 'type': 'INTEGER', 'mode': 'nullable'},
	{'name': 'totalspageviews', 'type': 'INTEGER', 'mode': 'nullable'},
	{'name': 'totalstimeOnSite', 'type': 'INTEGER', 'mode': 'nullable'},
	{'name': 'totalsbounces', 'type': 'INTEGER', 'mode': 'nullable'},
	{'name': 'totalstransactions', 'type': 'INTEGER', 'mode': 'nullable'},
]
TEMPLATE = '''
	(
		SELECT 
		'{2}' as origin 
		,hits.customDimensions.value as local_cid
		,visitNumber
		,visitStartTime
		,date
		,totals.visits
		,totals.hits
		,totals.pageviews
		,totals.timeOnSite
		,totals.bounces
		,totals.transactions
		FROM [{0}.{1}]
		where (hits.customDimensions.index = 2 and hits.customDimensions.value != '')
		GROUP EACH BY 1,2,3,4,5,6,7,8,9,10,11
	)'''
FINAL_SELECT = '''
	SELECT * FROM {0} ;
'''
COPY_COLUMNS = '''
	origin
	,local_cid
	,visitNumber
	,unix_time FILLER varchar(15)
	,visitStartTime AS TO_TIMESTAMP(unix_time)
	,date
	,totalsvisits
	,totalshits
	,totalspageviews
	,totalstimeOnSite
	,totalsbounces
	,totalstransactions
'''
DATASET_KEYS = {
	'48212628':'DE' # 'DE Analytics / http://www.westwing.de / Main Profile DE',
	,'51745462':'FR' # 'FR Analytics / FR Analytics / Main Profile FR',
	,'52892497':'IT' # 'IT Analytics / IT Analytics / Main Profile IT',
	,'52902509':'PL' # 'PL Analytics / PL Analytics / Main Profile PL',
	,'53349364':'BR' # 'BR Analytics / BR Analytics / Main Profile BR',
	,'53729292':'RU' # 'RU Analytics / RU Analytics / Main Profile RU',
	,'54621340':'NL' # 'NL Analytics / NL Analytics / Main Profile NL',
	,'55399154':'ES' # 'ES Analytics / ES Analytics / Main Profile ES',
	,'58508474':'CH' # 'CH Analytics / CH Analytics / Main Profile CH',
	,'73502928':'FR' # 'FR Analytics / FR Mobile App / Mobile App FR',
	,'73505727':'PL' # 'PL Analytics / PL Mobile App / Mobile App PL',
	,'73506930':'CH' # 'CH Analytics / CH Mobile App / Mobile App CH',
	,'73507518':'DE' # 'DE Analytics / DE Mobile App / Mobile App DE',
	,'73510731':'ES' # 'ES Analytics / ES Mobile App / Mobile App ES',
	,'73513727':'NL' # 'NL Analytics / NL Mobile App / Mobile App NL',
	,'73515515':'BR' # 'BR Analytics / BR Mobile App / Mobile App BR',
	,'73516226':'IT' # 'IT Analytics / IT Mobile App / Mobile App IT',
	,'73516625':'RU' # 'RU Analytics / RU Mobile App / Mobile App RU',
	,'82650736':'KZ' # 'KZ Analytics / KZ Analytics / Main Profile KZ',
	,'84038540':'KZ' # 'KZ Analytics / KZ Mobile App / Mobile App KZ',
	,'89564657':'CZ' # 'CZ Analytics / CZ Analytics / Main Profile CZ',
	,'89591923':'HU' # 'HU Analytics / HU Analytics / Main Profile HU',
	,'89759552':'CZ' # 'CZ Analytics / CZ Mobile App / Mobile App CZ',
	,'89760970':'HU' # 'HU Analytics / HU Mobile App / Mobile App HU',
	# ,'94482366':'DE' # 'DE Analytics / DE WestwingNow'
}
RECORDS_ALL = [
	'visitorId'
	,'fullVisitorId'
	,'visitNumber'
	,'visitId'
	,'visitStartTime'
	,'date'
	,'totals.visits'
	,'totals.hits'
	,'totals.pageviews'
	,'totals.timeOnSite'
	,'totals.bounces'
	,'totals.transactions'
	,'totals.transactionRevenue'
	,'totals.newVisits'
	,'totals.screenviews'
	,'totals.uniqueScreenviews'
	,'totals.timeOnScreen'
	,'trafficSource.referralPath'
	,'trafficSource.campaign'
	,'trafficSource.source'
	,'trafficSource.medium'
	,'trafficSource.keyword'
	,'trafficSource.adContent'
	,'device.browser'
	,'device.browserVersion'
	,'device.operatingSystem'
	,'device.operatingSystemVersion'
	,'device.isMobile'
	,'device.flashVersion'
	,'device.javaEnabled'
	,'device.language'
	,'device.screenColors'
	,'device.screenResolution'
	,'device.deviceCategory'
	,'geoNetwork.continent'
	,'geoNetwork.subContinent'
	,'geoNetwork.country'
	,'geoNetwork.region'
	,'geoNetwork.metro'
	,'customDimensions.index'
	,'customDimensions.value'
	,'hits.hitNumber'
	,'hits.time'
	,'hits.hour'
	,'hits.minute'
	,'hits.isSecure'
	,'hits.isInteraction'
	,'hits.referer'
	,'hits.page.pagePath'
	,'hits.page.hostname'
	,'hits.page.pageTitle'
	,'hits.page.searchKeyword'
	,'hits.page.searchCategory'
	,'hits.transaction.transactionId'
	,'hits.transaction.transactionRevenue'
	,'hits.transaction.transactionTax'
	,'hits.transaction.transactionShipping'
	,'hits.transaction.affiliation'
	,'hits.transaction.currencyCode'
	,'hits.transaction.localTransactionRevenue'
	,'hits.transaction.localTransactionTax'
	,'hits.transaction.localTransactionShipping'
	,'hits.transaction.transactionCoupon'
	,'hits.item.transactionId'
	,'hits.item.productName'
	,'hits.item.productCategory'
	,'hits.item.productSku'
	,'hits.item.itemQuantity'
	,'hits.item.itemRevenue'
	,'hits.item.currencyCode'
	,'hits.item.localItemRevenue'
	,'hits.contentInfo.contentDescription'
	,'hits.appInfo.name'
	,'hits.appInfo.version'
	,'hits.appInfo.id'
	,'hits.appInfo.installerId'
	,'hits.appInfo.appName'
	,'hits.appInfo.appVersion'
	,'hits.appInfo.appId'
	,'hits.appInfo.screenName'
	,'hits.appInfo.landingScreenName'
	,'hits.appInfo.exitScreenName'
	,'hits.appInfo.screenDepth'
	,'hits.exceptionInfo.description'
	,'hits.exceptionInfo.isFatal'
	,'hits.eventInfo.eventCategory'
	,'hits.eventInfo.eventAction'
	,'hits.eventInfo.eventLabel'
	,'hits.eventInfo.eventValue'
	,'hits.product.productSKU'
	,'hits.product.v2ProductName'
	,'hits.product.v2ProductCategory'
	,'hits.product.productVariant'
	,'hits.product.productBrand'
	,'hits.product.productRevenue'
	,'hits.product.localProductRevenue'
	,'hits.product.productPrice'
	,'hits.product.localProductPrice'
	,'hits.product.productQuantity'
	,'hits.product.productRefundAmount'
	,'hits.product.localProductRefundAmount'
	,'hits.product.isImpression'
	,'hits.promotion.promoId'
	,'hits.promotion.promoName'
	,'hits.promotion.promoCreative'
	,'hits.promotion.promoPosition'
	,'hits.refund.refundAmount'
	,'hits.refund.localRefundAmount'
	,'hits.eCommerceAction.action_type'
	,'hits.eCommerceAction.step'
	,'hits.eCommerceAction.option'
	,'hits.customVariables.index'
	,'hits.customVariables.customVarName'
	,'hits.customVariables.customVarValue'
	,'hits.customDimensions.index'
	,'hits.customDimensions.value'
	,'hits.customMetrics.index'
	,'hits.customMetrics.value'
	,'hits.social.socialInteractionNetwork'
	,'hits.social.socialInteractionAction'
]