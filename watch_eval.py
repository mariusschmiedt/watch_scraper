import os, sys
currentPath = os.path.dirname(os.path.abspath(__file__))

sys.path.append(currentPath)

from sqlConnection import sqlConnection

import pandas as pd
import numpy as np

def getValues(watches, searchStr, tableId):
	states = pd.DataFrame({'type': [],
		'share_abs': [],
		'share_perc': [],
		'avg': [],
		'std': [],
		'min': [],
		'max': []})
	values = watches[searchStr].unique()
	for idx in range(values.shape[0]):
		value = values[idx]
		cluster = watches[watches[searchStr] == value]
		share_abs = cluster.shape[0]
		share_perc = np.round((cluster.shape[0] / watches.shape[0]) * 100, 2)
		prices = pd.to_numeric(cluster['price_euro'], errors='coerce')
		avg_price = prices.mean(skipna=True)
		price_std = prices.std(skipna=True)
		min_price = prices.min(skipna=True)
		max_price = prices.max(skipna=True)
		state = pd.DataFrame({'type': value,
			'share_abs': share_abs,
			'share_perc': share_perc,
			'avg': avg_price,
			'std': price_std,
			'min': min_price,
			'max': max_price}, index=[idx])
		states = pd.concat([states, state])
	if searchStr == 'model_reference':
		states = postProcessValues(watches, states, searchStr)
	if searchStr == 'model' or searchStr == 'model_reference':
		searchStr = 'model'
		states = postProcessValues(watches, states, searchStr)
	
	tableName = "watch_stats_global"
	if tableId != '':
		tableName = "watch_stats_" + tableId
	sql = sqlConnection()
	sql.cur.execute("CREATE TABLE IF NOT EXISTS [" + tableName + "]")
	for state in states:
		pass
	sql.closeMySQL()
	return states

def postProcessValues(watches, states, searchStr):
	states = states.rename(columns={'type': searchStr})
	compareStr = ''
	if searchStr == 'model':
		compareStr = 'brand'
	elif searchStr == 'model_reference':
		compareStr = 'model'
	values = []
	for model in states[searchStr]:
		value = ''
		foundValues = watches[watches[searchStr] ==  model][compareStr].unique()
		if foundValues.shape[0] == 1:
			value = foundValues[0]
		else:
			counts = [watches[compareStr][watches[compareStr] == b].count() for b in foundValues]
			value = foundValues[counts.index(max(counts))]
		values.append(value)
	states[compareStr] = values
	return states

def watchEval(states):
	mostSuppliedWatch = states[states['share_perc'] == states['share_perc'].max()]
	leastSuppliedWatches = states[states['share_perc'] == states['share_perc'].min()]
	minShareStates = states[states['share_abs'] > 10]
	stabilityStates = minShareStates.sort_values('std', ignore_index=True)
	exclusiveStates = minShareStates.sort_values('avg', ascending=False, ignore_index=True)
	rareIdx = [sIdx + exclusiveStates[exclusiveStates['model_reference'] == stabilityStates.iloc[sIdx]['model_reference']].index[0] for sIdx in range(stabilityStates.shape[0])]
	sortRareIdx = rareIdx.copy()
	sortRareIdx.sort()
	stableExclusiveWatches = stabilityStates.copy()
	stableExclusiveWatches = stableExclusiveWatches.iloc[0:0]
	for s in sortRareIdx:
		foundWatch = stabilityStates.loc[stabilityStates['model_reference']==stabilityStates.iloc[rareIdx.index(s)]['model_reference']]
		if stableExclusiveWatches.empty:
			stableExclusiveWatches = foundWatch
		else:
			stableExclusiveWatches = pd.concat([stableExclusiveWatches, foundWatch])
	stableExclusiveWatches = stableExclusiveWatches.reset_index()
	lowPriceStable = stableExclusiveWatches[(stableExclusiveWatches['avg'] < 20000) & (stableExclusiveWatches['share_abs'] > 50)]
	lowPriceStable[lowPriceStable['avg'] == lowPriceStable['avg'].min()]
	return mostSuppliedWatch, leastSuppliedWatches, stableExclusiveWatches

def extractPossibilities(watches, states):
	for state in states:
		
		priceLowerAvg = watches[(watches['model_reference'] == state['model_reference']) & (pd.to_numeric(watches['price_euro'], errors='coerce') < state['avg'])]
		priceLowerAvg[pd.to_numeric(priceLowerAvg['price_euro']) == pd.to_numeric(priceLowerAvg['price_euro']).min()]

def watchEvaluation():
	sql = sqlConnection()
	watches = pd.read_sql('SELECT * FROM watch_scrape', con=sql.mydb)
	sql.closeMySQL()
	globalBrandStates = getValues(watches, 'brand', '')
	globalModelStates = getValues(watches, 'model', '')
	globalReferenceStates  = getValues(watches, 'model_reference', '')
	
	countries = watches['location'].unique()
	for country in countries:
		locWatches = watches[watches['location'] == country]
		localBrandStates = getValues(locWatches, 'brand', country)
		localModelStates = getValues(locWatches, 'model', country)
		localReferenceStates  = getValues(locWatches, 'model_reference', country)


watchEvaluation()


# mostSuppliedWatch, leastSuppliedWatches, stableExclusiveWatches = watchEval(referenceStates)

