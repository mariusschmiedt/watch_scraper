#!/usr/bin/env python
import os, sys
currentPath = os.path.dirname(os.path.abspath(__file__))

sys.path.append(currentPath)

from sqlConnection import sqlConnection
import time
from bs4 import BeautifulSoup as BS
from multiprocessing import Process, Pipe
import requests
import random
import re
import math
import datetime
import pandas as pd

def generate_urls(search_str, brand):
	search_str_list = list()
	
	# get elements on page and determine pages
	entriesPerSite = 0
	numResults = 0
	pages = 0
	
	# get web content
	soup = gerSrcCode(search_str)
	
	# if chrono24 is scraped
	if "chrono24" in search_str:
		# check header for script tag
		scriptCrawl = [a for a in soup.findAll('script')]
		
		# take second script tag content
		scriptContent = scriptCrawl[1].contents[0]
		
		# delete content until sub string
		headerInfo = scriptContent[scriptContent.find("numResult"):-1]
		
		# delete content after substring to get desired sub string and split in list
		headerInfo = headerInfo[0:headerInfo.find("}")].replace('"','').split(',')
		
		# get infotmation from sub string
		numResults = float(headerInfo[0].split(':')[1])
		entriesPerSite = float(headerInfo[1].split(':')[1])
		
		# determine number of pages
		pages = math.ceil(numResults / entriesPerSite)
		
		# build urls to scrape
		search_str_list.append(search_str)
		search_str_list = search_str_list + ["https://www.chrono24.de/" + brand + "/index-" + str(i) + ".htm?man=" + brand + "&showpage=&sortorder=5" for i in range(2,pages)]
	
	return search_str_list
	

def gerSrcCode(search_url):
	
	# header to be sent to hide python scrape
	user_agents_list = [
		'Mozilla/5.0 (iPad; CPU OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148',
		'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.83 Safari/537.36',
		'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36'
	]
	
	res = requests.get(search_url, headers={'User-Agent': random.choice(user_agents_list)}).text
	soup = BS(res,'html5lib')
	return soup

def scrapeUrl(searchStr, watches, offers):
	soup = None
	try:
		soup = gerSrcCode(searchStr)
	except:
		pass
	if soup is not None:
		if "chrono24" in searchStr:
			scrapeChronoTf(soup, watches, offers)

def scrapeChronoTf(soup, watches, offers):
	# get section where offers are stored
	pageContent = soup.find('script', attrs={'type':'application/ld+json'}).contents[0]
	
	# delete part of string till sub string
	offerUnstructured = pageContent[pageContent.find("offers"):-1]
	
	# extract only relevant offers, clean format, split lines to list
	offerCleaned = offerUnstructured[offerUnstructured.find("["):offerUnstructured.find("]")].replace('\t', '').replace('"', '').split('\n')
	
	# read lines and extract urls of offers
	urls = [o.split('url:')[1] + '?SETLANG=en_DE&SETCURR=EUR' for o in offerCleaned if o.startswith('url')]
	# loop offer urls
	for url in urls:
		curDateStr = str(datetime.datetime.now().strftime("%Y-%m-%d"))

		action = True
		if url in offers['url'].tolist():
			foundOffer = offers[offers['url'] == url]
			curDate = datetime.datetime.strptime(curDateStr, "%Y-%m-%d")
			compDate = datetime.datetime.strptime(foundOffer['in_db_since'].values[0], "%Y-%m-%d")
			if foundOffer['price_change_detected_on'].values[0] != '':
				compDate = datetime.datetime.strptime(foundOffer['price_change_detected_on'].values[0], "%Y-%m-%d")
			if curDate - datetime.timedelta(days=20) < compDate:
				action = False
				
		if action:
			# define empty state (passed to database)
			watchState = {
				"brand":"",
				"model":"",
				"model_reference":"",
				"material_case":"",
				"material_bezel":"",
				"material_bracelet":"",
				"material_clasp":"",
				"color_dial":"",
				"color_bracelet":"",
				"crystal":"",
				"calibre":"",
				"diameter":"",
				"numerals_dial":"",
				"gender":"",
				"power_reserve":"",
				"water_resistance":"",
				"number_of_jewels":"",
				"clasp":"",
				"movement":"",
			}

			# define empty state (passed to database)
			offerState = {
				"platform":"",
				"platform_id":"",
				"year_of_manufacture":"",
				"condition_of_use":"",
				"scope_of_delivery":"",
				"location":"",
				"price_euro":"",
				"in_db_since":"",
				"price_change_detected_on":"",
				"price_changes":"",
				"active_status":"",
				"initial_price_detected":"",
				"url": "",
				"current_date":""
			}
			
			# get source code of offer page
			soupPage = gerSrcCode(url)
			
			# get relevant section of offer
			offerContent = soupPage.find('div', attrs={'class':'js-tab tab'})
			
			# extract only relevant content of offer
			offerContent = offerContent.find('table')
			
			for match in offerContent.findAll('tr'):
				if len(match.findAll('td')) != 2:
					match.extract()
			
			# remove unnecessary tags and content
			removeTags = ['script', 'h3']
			for tag in removeTags:
				for match in offerContent.findAll(tag):
					match.extract()
			
			# remove unnecessary tags but keep content
			ignoreTags = ['b', 'i', 'u', 'strong', 'a', 'div', 'span']
			for tag in ignoreTags:
				for match in offerContent.findAll(tag):
					match.replaceWithChildren()
			
			
			# remove attributes from relevant tags and remove  empty tags
			for e in offerContent.findAll('td'):
				e.attrs = {}
				if len(e.get_text(strip=True)) == 0 and e.name not in ['br', 'img']:
					e.extract()
			
			# get relevant tags
			offerSpec = offerContent.findAll('td')
			
			# pre process values
			offerValues = list()
			for a in offerSpec:
				value = a.text.strip()
				value = value.replace('"', '')
				value = value.replace("'", '')
				value = value.replace(' ATM', '0')
				value = value.replace(' mm', '')
				value = value.replace(' h','')
				if (u"\u20AC" in value or '€' in value) and '(' in value:
					value = value.split('(')[1]
				if u"\u20AC" in value or '€' in value:
					value = value.replace(')','').replace('=','').replace(u"\u20AC",'').replace("€",'')
					if '[' in value:
						value = value.split('[')[0]
				value = value.split('\n')[0].strip()
				offerValues.append(value)
			
			# assign elements to lists
			description = list()
			content = list()
			for i in range(len(offerValues)):
				if i % 2:
					content.append(offerValues[i])
				else:
					description.append(offerValues[i])
			
			# shorten the longer list (elements without description)
			if len(description) > len(content):
				description = description[0:len(content)]
			if len(description) < len(content):
				content = content[0:len(description)]
			
			# assign values to state
			offerState["platform"] = "chrono24"
			offerState["current_date"] = curDateStr
			offerState["url"] = url
			# assign values of website to state
			for i in range(len(description)):
				des = description[i]
				con = content[i]
				if des == 'Listing code':
					offerState["platform_id"] = con
				elif des == 'Brand':
					watchState["brand"] = con
				elif des == 'Model':
					watchState["model"] = con
				elif des == 'Reference number':
					watchState["model_reference"] = con
				elif des == 'Case material':
					watchState["material_case"] = con
				elif des == 'Bracelet material':
					watchState["material_bracelet"] = con
				elif des == 'Clasp material':
					watchState["material_clasp"] = con
				elif des == 'Year of production':
					offerState["year_of_manufacture"] = con
				elif des == 'Condition':
					offerState["condition_of_use"] = con
				elif des == 'Scope of delivery':
					offerState["scope_of_delivery"] = con
				elif des == 'Gender':
					watchState["gender"] = con
				elif des == 'Location':
					offerState["location"] = con
				elif des == 'Price':
					offerState["price_euro"] = con
				elif des == 'Movement/Caliber':
					watchState["calibre"] = con
				elif des == 'Power reserve':
					watchState["power_reserve"] = con
				elif des == 'Case diameter':
					watchState["diameter"] = con
				elif des == 'Water resistance':
					watchState["water_resistance"] = con
				elif des == 'Bezel material':
					watchState["material_bezel"] = con
				elif des == 'Number of jewels':
					watchState["number_of_jewels"] = con
				elif des == 'Crystal':
					watchState["crystal"] = con
				elif des == 'Dial':
					watchState["color_dial"] = con
				elif des == 'Dial numerals':
					watchState["numerals_dial"] = con
				elif des == 'Bracelet color':
					watchState["color_bracelet"] = con
				elif des == 'Clasp':
					watchState["clasp"] = con
				elif des == 'Movement':
					watchState["movement"] = con

			# process empty fields
			watchState = processCalibre(watchState)
			watchState = processModel(watchState, watches)
			watchState = processReference(watchState, offerState, watches)
			watchState = processBrand(watchState, watches)
			watchState = processDiameter(watchState)
			# process fields in general with closest findings
			similarFindings = similarWatches(watchState, offerState, watches)
			if similarFindings.shape[0] > 0:
				watchState = processKey(watchState, similarFindings, 'gender')
				watchState = processKey(watchState, similarFindings, 'clasp')
				watchState = processKey(watchState, similarFindings, 'movement')
				watchState = processKey(watchState, similarFindings, 'water_resistance')
				watchState = processKey(watchState, similarFindings, 'power_reserve')
				# process remaining empty fields
				watchState = findAttributesFromReference(watchState, similarFindings)
			# process other
			offerState = processLocation(offerState)
			offerState = processPrice(offerState)
			offerState = processYear(offerState)
			if watchState['model'] != '' and watchState['brand'] != '' and watchState['model_reference'] != '' and offerState["price_euro"] != '':
				writeToDb(watchState, offerState)
	
def processPrice(state):
	initialPrice = state["price_euro"]
	state["price_euro"] = initialPrice.replace('.','').replace(',','.')
	try:
		float(state["price_euro"])
	except:
		state["price_euro"] = ''
	return state

def processLocation(state):
	initialLocation = state["location"]
	state["location"] = initialLocation.split(',')[0].strip().replace(' ', '-').lower()
	return state

def processYear(state):
	if len(state['year_of_manufacture'].split(' ')) > 1:
		year = ''
		for elem in state['year_of_manufacture'].split(' '):
			try:
				year = str(int(elem))
			except:
				pass
		if year != '':
			state['year_of_manufacture'] = year
	return state

def processCalibre(state):
	initialCalibre = state["calibre"]
	calibre = initialCalibre.strip()
	calibre = calibre.replace(' ', '-')
	if '-' in calibre:
		for cal in calibre.split('-'):
			numbers = sum(c.isdigit() for c in cal)
			if numbers == 4:
				calibre = cal
	numbers = sum(c.isdigit() for c in calibre)
	if numbers != len(calibre):
		calibre = ''
	state["calibre"] = calibre
	return state

def processModel(state, watches):
	if state['model'] == '':
		model = ''
		modelFindings = []
		if state['model_reference'] != '':
			# mydb, cur = openMySQL()
			# referenceFindings = pd.read_sql("SELECT * FROM offers WHERE model_reference='" + state['model_reference'] + "'", con=mydb)
			# closeMySQL(mydb, cur)
			referenceFindings = watches[watches['model_reference'] == state['model_reference']]
			modelFindings = referenceFindings['model'].unique()
			if modelFindings.shape[0] == 1:
				model = modelFindings[0]
			elif modelFindings.shape[0] == 2:
				if '' in modelFindings:
					model = modelFindings[modelFindings != ''][0]
		else:
			# mydb, cur = openMySQL()
			# calibreFindings = pd.read_sql("SELECT * FROM offers WHERE calibre='" + state['calibre'] + "'", con=mydb)
			# closeMySQL(mydb, cur)
			calibreFindings = watches[watches['calibre'] == state['calibre']]
			calibreFindings = calibreFindings.loc[calibreFindings['brand'] == state['brand']]
			modelFindings = calibreFindings['model'].unique()
			if modelFindings.shape[0] == 1:
				model = modelFindings[0]
			elif modelFindings.shape[0] == 2:
				if '' in modelFindings:
					model = modelFindings[modelFindings != ''][0]
				else:
					models = modelFindings['model'].tolist()
					for idx in range(len(models)):
						modelTemp = models[idx]
						stateTemp = state.copy()
						stateTemp['model'] = modelTemp
						stateTemp = processReference(stateTemp, watches)
						if stateTemp['model_reference'] != '':
							model = stateTemp['model']
							break
		if model != '':
			state['model'] = model
	if '-' in state['model'] or '_' in state['model']:
		state['model'] = state['model'].replace('-', ' ').replace('_', ' ').strip()
	
	return state

def processReference(watchState, offerState, watches):

	# remove everything from the model reference which is not typical for the reference value
	initialReference = watchState["model_reference"]
	ref_split = initialReference.upper().split(' ')
	new_ref = ''
	if len(ref_split) > 1:
		for idx in range(len(ref_split)):
			numbers = sum(c.isdigit() for c in ref_split[idx])
			letters = sum(c.isalpha() for c in ref_split[idx])
			noSave = False
			if numbers == 4 and letters == 0:
				for n in range(len(ref_split[idx])):
					if not ref_split[idx][-1].isdigit():
						ref_split[idx] = ref_split[idx][:-1]
					if not ref_split[idx][0].isdigit():
						ref_split[idx] = ref_split[idx][1:]
					if ref_split[idx][-1].isdigit() and ref_split[idx][0].isdigit():
						break
				try:
					if int(ref_split[idx]) in range(1900, int(datetime.datetime.now().strftime("%Y"))+1):
						noSave = True
				except:
					pass
				if idx > 1:
					if ref_split[idx-1].lower() == 'year':
						noSave = True
			if numbers > 2 and noSave == False:
				new_ref = new_ref + ' ' + re.sub('^\\D*', '', ref_split[idx]).upper()
	if new_ref != '':
		watchState["model_reference"] = new_ref.strip()

	# if the model reference is empty find it by it the attributes of the watch
	if watchState['model_reference'] == '':
		brand = watchState['brand']
		model = watchState['model']
		model_reference = ''
		material_case = watchState['material_case']
		material_bezel = watchState['material_bezel']
		material_bracelet = watchState['material_bracelet']
		calibre = watchState['calibre']
		color_dial = watchState['color_dial']
		color_bracelet = watchState['color_bracelet']
		year_of_manufacture = offerState['year_of_manufacture']
		if brand != '' and model != '':
			# mydb, cur = openMySQL()	
			# closestFindings = pd.read_sql("SELECT * FROM offers  WHERE brand = '" + brand + "' AND model = '" + model + "' AND model_reference <> ''", con=mydb)
			# closeMySQL(mydb, cur)
			closestFindings = watches[(watches['brand'] == brand) & (watches['model'] == model) & (watches['model_reference'] != '')]
			tempClosestFindings = closestFindings.copy()
			if closestFindings.shape[0] > 0:
				tempClosestFindings = closestFindings.loc[(closestFindings['material_case'] == material_case) | (closestFindings['material_case'] == '')]
			if tempClosestFindings.shape[0] > 0:
				closestFindings = tempClosestFindings.copy()
				tempClosestFindings = closestFindings.loc[(closestFindings['material_bezel'] == material_bezel) | (closestFindings['material_bezel'] == '')]
			if tempClosestFindings.shape[0] > 0:
				closestFindings = tempClosestFindings.copy()
				tempClosestFindings = closestFindings.loc[(closestFindings['material_bracelet'] == material_bracelet) | (closestFindings['material_bracelet'] == '')]
			if tempClosestFindings.shape[0] > 0:
				closestFindings = tempClosestFindings.copy()
				tempClosestFindings = closestFindings.loc[(closestFindings['calibre'] == calibre) | (closestFindings['calibre'] == '')]
			if tempClosestFindings.shape[0] > 0:
				closestFindings = tempClosestFindings.copy()
				tempClosestFindings = closestFindings.loc[(closestFindings['color_bracelet'] == color_bracelet) | (closestFindings['color_bracelet'] == '')]
			if tempClosestFindings.shape[0] > 0:
				closestFindings = tempClosestFindings.copy()
				tempClosestFindings = closestFindings.loc[(closestFindings['color_dial'] == color_dial) | (closestFindings['color_dial'] == '')]
			if tempClosestFindings.shape[0] > 0:
				closestFindings = tempClosestFindings.copy()
			
			if closestFindings.shape[0] > 0:
				# scoreList = np.zeros((closestFindings.shape[0], 1))
				scoreList = list()
				material_cases = closestFindings['material_case'].tolist()
				material_bezels = closestFindings['material_bezel'].tolist()
				material_bracelets = closestFindings['material_bracelet'].tolist()
				calibres = closestFindings['calibre'].tolist()
				color_bracelets = closestFindings['color_bracelet'].tolist()
				color_dials = closestFindings['color_dial'].tolist()
				
				for idx2 in range(closestFindings.shape[0]):
					curScore = 0
					if material_cases[idx2] != '':
						curScore += 1
					if material_bezels[idx2] != '':
						curScore += 1
					if material_bracelets[idx2] != '':
						curScore += 1
					if calibres[idx2] != '':
						curScore += 1
					if color_bracelets[idx2] != '':
						curScore += 1
					if color_dials[idx2] != '':
						curScore += 1
					# scoreList[idx2, 0] = curScore
					scoreList.append(curScore)
				
				# if np.max(scoreList) >= 3:
				if max(scoreList) >= 3:
					closestFindings = closestFindings.iloc[scoreList.index(max(scoreList))]
					if closestFindings.shape[0] == 1:
						model_reference = closestFindings['model_reference'].tolist()[0]
					elif closestFindings.shape[0] > 1:				
						# yearFind = closestFindings.loc[(pd.to_numeric(closestFindings['year_of_manufacture'], errors='coerce') < pd.to_numeric(year_of_manufacture, errors='coerce') + 5) & (pd.to_numeric(closestFindings['year_of_manufacture'], errors='coerce') > pd.to_numeric(year_of_manufacture, errors='coerce') - 5)]
						# if yearFind.shape[0] > 0:
						# 	model_reference = yearFind['model_reference'].tolist()[0]
						if model_reference == '':
							refList = [mRef.split('-')[0] for mRef in closestFindings['model_reference']]
							refListUnique = list(set(refList))
							refCount = [refList.count(refListUnique[idx]) for idx in range(len(refListUnique))]
							model_reference = refListUnique[refCount.index(max(refCount))]
				
				if model_reference != '':
					watchState['model_reference'] = model_reference
	if watchState['model_reference'] != '':
		# mydb, cur = openMySQL()
		# referenceFindings = pd.read_sql("SELECT * FROM offers WHERE model_reference='" + state['model_reference'] + "'", con=mydb)
		# closeMySQL(mydb, cur)
		referenceFindings = watches[watches['model_reference'] == watchState['model_reference']]
		if referenceFindings['model'].unique().shape[0] > 1:
			watchState['model'] = referenceFindings['model'].mode().values[0]
	return watchState

def processBrand(state, watches):
	if state['model'] != '' and state['model_reference'] != '':
		brandWatches = watches[(watches['model'] == state['model']) & (watches['model_reference'] == state['model_reference'])]
		if brandWatches['brand'].unique().shape[0] > 1:
			if brandWatches['brand'].mode().values[0] != state['brand']:
				state['brand'] = brandWatches['brand'].mode().values[0]
	return state

def processDiameter(state):
	if state['diameter'] == '' and len(state['model'].split(' ')) > 1:
		numbers = [sum(c.isdigit() for c in mod.strip()) for mod in state['model'].split(' ')]
		if 2 in numbers:
			diaIdx = numbers.index(2)
			try:
				state['diameter'] = str(int(state['model'].split(' ')[diaIdx]))
			except:
				pass
	return state

def processKey(state, watches, key):
	if watches[key].unique().shape[0] > 1:
		state[key] = watches[key].mode().values[0]
	return state

def similarWatches(watchState, offerState, watches):
	closestFindings = watches[(watches['brand'] == watchState['brand']) & (watches['model'] == watchState['model']) & (watches['model_reference'] == watchState['model_reference'])]
	tempClosestFindings = closestFindings.copy()
	if closestFindings.shape[0] > 0:
		tempClosestFindings = closestFindings[(closestFindings['calibre'] == watchState['calibre'])]
	if tempClosestFindings.shape[0] > 0:
		closestFindings = tempClosestFindings.copy()
		tempClosestFindings = closestFindings[(closestFindings['diameter'] == watchState['diameter'])]
	'''
	if tempClosestFindings.shape[0] > 0:
		closestFindings = tempClosestFindings.copy()
		closestOffers = closestFindings[closestFindings['id'] == offerState['watch_id']]

		year_of_manufacture_eq = pd.to_numeric(offerState['year_of_manufacture'], errors='coerce')
		year_of_manufacture = pd.to_numeric(closestOffers["year_of_manufacture"], errors='coerce')
		closestOffers = closestOffers.loc[(year_of_manufacture_eq < year_of_manufacture + 5) & (year_of_manufacture_eq > year_of_manufacture - 5)]
		tempClosestFindings = closestFindings[closestFindings['id'].isin(closestOffers['watch_id'].tolist())]
	'''
	if tempClosestFindings.shape[0] > 0:
		closestFindings = tempClosestFindings.copy()
	
	return closestFindings

def findAttributesFromReference(state, watches):
	if state['model_reference'] != '':
		toBeObserved = watches.columns.drop(['id', 'brand', 'model', 'model_reference'])
		pdState = pd.Series(state)
		if pdState[toBeObserved][pdState[toBeObserved] == ''].shape[0] >= 1:
			emptyKeys = pdState[toBeObserved][pdState[toBeObserved] == ''].keys().tolist()
			# selection = ', '.join(emptyKeys)

			# mydb, cur = openMySQL()
			# attrFindings = pd.read_sql("SELECT " + selection + " FROM offers WHERE model_reference='" + state['model_reference'] + "'", con=mydb)
			# closeMySQL(mydb, cur)
			for eKey in emptyKeys:
				state[eKey] = watches[eKey].mode()[0]
				
	return state

def writeToDb(watchState, offerState):
	sql = sqlConnection()
	
	# query to check if ad entry is already in database
	sql.cur.execute("SELECT id FROM offers WHERE platform_id = '" + offerState["platform_id"] + "'")	
	existingID = sql.cur.fetchall()

	# check if watch is already existing
	watchKeys = list(watchState.keys())
	select_str = ' AND '.join([watchKeys[idx] + " = '" + watchState[watchKeys[idx]] + "'" for idx in range(len(watchKeys))])
	sql.cur.execute("SELECT id FROM watches WHERE " + select_str)
	watchId = sql.cur.fetchall()
	# add entry to watches if watch does not exist
	if len(watchId) == 0:
		key_str = ", ".join([watchKeys[idx] for idx in range(len(watchKeys))])
		value_str = ", ".join(["'" + watchState[watchKeys[idx]] + "'" for idx in range(len(watchKeys))])
		sql.cur.execute("INSERT INTO watches (" + key_str + ") VALUES (" + value_str + ")")
		
		# get id of watch
		sql.cur.execute("SELECT id FROM watches WHERE " + select_str)
		watchId = sql.cur.fetchall()
	watchId = watchId[0][0]
	'''
	# query to check if an entry is similar in database existing
	sql.cur.execute("SELECT id FROM offers WHERE watch_id = " + str(watchId[0]) + " AND price_euro = '"+ offerState["price_euro"] + "'")
	similarID = sql.cur.fetchall()
	'''
	
	# if nothing is found add database entry otherwise update the value
	if len(existingID) == 0:# and len(similarID) == 0:
		# SQL query to INSERT a new record into the table result_list.
		sql.cur.execute("INSERT INTO offers (watch_id, platform, platform_id, year_of_manufacture, condition_of_use, scope_of_delivery, location, price_euro, in_db_since, price_change_detected_on, price_changes, active_status, initial_price_detected, url) VALUES (" + str(watchId) + ",'" + offerState["platform"] + "','" + offerState["platform_id"] + "','" + offerState["year_of_manufacture"] + "','" + offerState["condition_of_use"] + "','" + offerState["scope_of_delivery"] + "','" + offerState["location"] + "','" + offerState["price_euro"] + "','" + offerState["current_date"] + "','" + offerState["price_change_detected_on"] + "',0,1,'" + offerState["price_euro"] + "','" + offerState["url"] + "')")
	else:
		if len(existingID) != 0:
			# id of the existing entry
			dbId = str(existingID[0][0])
			# select significant values to detect changes
			sql.cur.execute("SELECT watch_id, price_euro, in_db_since, price_changes FROM offers WHERE id = " + dbId + "")
			
			ad_result = sql.cur.fetchone()
			# extract the current database values
			watch_id = ad_result[0]
			initial_price_detected = ad_result[1]
			in_db_since = ad_result[2]

			sql.cur.execute("SELECT brand, model, model_reference FROM watches WHERE id = " + str(watch_id) + "")

			watch_result = sql.cur.fetchone()
			# extract the current database values
			brand = watch_result[0]
			model = watch_result[1]
			model_reference = watch_result[2]
			
			# compare database values with current values
			if brand != watchState["brand"] or model != watchState["model"] or model_reference != watchState["model_reference"] or initial_price_detected != offerState["price_euro"]:
				
				# price changes
				price_changes = str(int(ad_result[3]) + 1)
				
				# if the price did not change
				if initial_price_detected == offerState["price_euro"]:
					price_changes = str(ad_result[3])
				
				# if the price has changed but nothing else has changed only update the price
				if initial_price_detected != offerState["price_euro"] and brand == watchState["brand"] and model == watchState["model"] and model_reference == watchState["model_reference"]:
					
					sql.cur.execute("UPDATE offers SET price_euro = '" + offerState["price_euro"] + "', in_db_since = '" + in_db_since + "', price_change_detected_on = '" + offerState["current_date"] + "', price_changes = " + price_changes + ", initial_price_detected = '" + initial_price_detected + "' WHERE ID = " + dbId + '')
				
				# if the other values also have changed update the entire entry
				else:
					# if the price has changed
					if initial_price_detected != offerState["price_euro"]:
						
						sql.cur.execute("UPDATE offers SET watch_id = " + str(watchId) + ", year_of_manufacture = '" + offerState["year_of_manufacture"] + "', condition_of_use = '" + offerState["condition_of_use"] + "', scope_of_delivery = '" + offerState["scope_of_delivery"] + "', location = '" + offerState["location"] + "', price_euro = '" + offerState["price_euro"] + "', in_db_since = '" + in_db_since + "', price_change_detected_on = '" + offerState["current_date"] + "', price_changes = " + price_changes + ", active_status = 1, initial_price_detected = '" + initial_price_detected + "', url = '" + offerState["url"] + "' WHERE ID = " + dbId + '')
						
					# if the price has not changed
					else:
						
						sql.cur.execute("UPDATE offers SET watch_id = " + str(watchId) + ", year_of_manufacture = '" + offerState["year_of_manufacture"] + "', condition_of_use = '" + offerState["condition_of_use"] + "', scope_of_delivery = '" + offerState["scope_of_delivery"] + "', location = '" + offerState["location"] + "', price_euro = '" + offerState["price_euro"] + "', in_db_since = '" + in_db_since + "', price_change_detected_on = '" + offerState["price_change_detected_on"] + "', price_changes = " + price_changes + ", active_status = 1, initial_price_detected = '" + offerState["price_euro"] + "', url = '" + offerState["url"] + "' WHERE ID = " + dbId + '')
	
	sql.closeMySQL()

def chronoTfBrandUrl(brand):
	chronoLink = "https://www.chrono24.de/" + brand + "/index.htm?man=" + brand + "&showpage=&sortorder=5"
	searchStrList = generate_urls(chronoLink, brand)
	return searchStrList

def preProcessDbTables():
	# tables to be modified
	tables=["offers", "watches"]

	sql = sqlConnection()

	# modify index (get last idx)
	for t in tables:
		last_id = 1
		try:
			sql.cur.execute("SELECT MAX(id) FROM " + t)
			found_ids = sql.cur.fetchall()
			if len(found_ids) > 0:
				last_id = int(found_ids[0][0])
		except:
			last_id = 1
		sql.cur.execute("ALTER TABLE " + t + " AUTO_INCREMENT = " + str(last_id))
	
	sql.closeMySQL()

def preProcessTableEntries():
	# select offers
	sql = sqlConnection()
	offers = pd.read_sql('SELECT * FROM offers', con=sql.mydb)

	ids = offers['id'].tolist()
	urls = offers['url'].tolist()

	for idx in range(len(urls)):
		url = urls[idx]
		response = requests.get(url)
		if response.status_code != 200:
			sql.cur.execute("UPDATE offers SET active_status = 0 WHERE ID = " + str(ids[idx]) + '')

	sql.closeMySQL()


# searches to be scraped
brands = ["rolex", "audemarspiguet", "patekphilippe"]

preProcessDbTables()
preProcessTableEntries()

searchStrList = list()
for brand in brands:
	brandList = chronoTfBrandUrl(brand)
	searchStrList = searchStrList + brandList

if __name__ == '__main__':
	idx = 0
	pipe_length = 10
	while idx < len(searchStrList):
		sql = sqlConnection()
		watches = pd.read_sql('SELECT * FROM watches', con=sql.mydb)
		offers = pd.read_sql('SELECT * FROM offers', con=sql.mydb)
		sql.closeMySQL()
		
		chunk_end = min(idx + pipe_length, len(searchStrList))
		searchChuncks = searchStrList[idx:chunk_end]
		processes = []
		parent_connections = []
		for searchStr in searchChuncks:	
			parent_conn, child_conn = Pipe()
			child_conn.close()
			parent_connections.append(parent_conn)
			process = Process(target=scrapeUrl, args=(searchStr,watches,offers,))
			processes.append(process)
		
		for process in processes:
			process.start()
		
		for process in processes:
			process.join()
		
		idx = idx + pipe_length