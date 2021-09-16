# 
# K2S-O: Real-time, multi-processed, multi-threaded time-series anomaly detection
# 
# K2so pulls data from a time-series database on a pre-defined timing schedule, adding to an in-memory buffer each time.
# Median and wavelet filters are applied against the in-memory buffer to reduce noise. The resultant signal is then detrended
# using STLOESS and then run through a Season Hybrid Extreme Studentized Deviate to assess the waveform for statistical
# anomalies. Those anomalies are returned, paired with the original signal (as well as the filtered signal for cross-referencing) and
# then reported to OSNDS's alerting API. Logic has been added to group nomalies together into "events"; this is not an association approach,
# but rather a nearby clustering method prescribed by an end-user's set "reset time window". This limitation is intentional as we have aimed to 
# make this code as applicable to multiple mission areas as possible. Performing association would require us to make certain assumptions 
# of either the originating event of interest or the phenomenology of the data collection method. We highly encourage researchers to fork this 
# code and embed their own association algorithims.  
# 
# This python script was collaboratively written by members of AFTAC/SI (James Stroup, Ed Robbins, and Samuel Stevens), based upon the excellent work
# (written in R) by Ed Robbins (AFTAC/SI) which leveraged the foundational work of Twitter's Reasearch Team (as well as numerous open-source packages).
# Please see the end of this file for a full list of usage credits.
# 
# Usage instructions:
# 
# This code is ultimately called and executed from another script "k2so.py.  You can call k2so.py via:
# 
#     python k2so -s [stations]
#
# Wherein the "-s" is an argument flag for "stations", as in which stations you would like k2so to monitor against. 
# You must follow this flag by each station's ID, separated by spaces (single-word alphaumerics are accepted). For example:
# 
#     python k2so.py -s 1 2 3 4 X1 X12
# 
# We will soon be adding a "-d" argument to the scripts execution that will force K2SO to operate in a DEBUG MODE. This mode will provide the
# end user with a log of K-2SO's output labed by Station ID. This feature is still in work. 
#

import warnings
from numpy.core.numeric import NaN
#from scipy.signal import waveforms, wavelets
from logic_files import k2s0_file_handler as k2_file

warnings.simplefilter(action='ignore', category=FutureWarning)

import tad
import scipy
import pandas as pd
import matplotlib.pyplot as plt
import os, math, time, requests, json

from influxdb import DataFrameClient
from twisted.internet import task, reactor

from skimage.restoration import denoise_wavelet, estimate_sigma

pd.set_option('display.max_rows', 1000)




class DataStore():

	last_event_id = 0

	previous_valid_first_index = 0
	previous_valid_first_value = 0

	previous_valid_last_index = 0
	previous_valid_last_value = 0

	waveform = pd.DataFrame()
	
	med_x = None
	med_y = None
	med_z = None

	report_ID_buffer = []

class InfluxStore():

	client = None
	query_median = ""
	query_data = ""

class Settings():

	config = None
	station = 0
	trigger_cooldown = 0
	debug = False
	filter_coefficients = None



# create instances for each class, therby passing initial values for each downtsream variable
settings = Settings()
influx = InfluxStore()
data = DataStore()



def initialize_k2so():

	kill = False

	# All of k2so's settings (with the exception of which stations you;re running against) are assignable in the JSON file
	# This function loads the JSON file and stores all of the user settings as a list, "config"
	
	station_configuration = str('configuration_files/k2so_configuration_osnds_'+settings.station+'.json')

	print('\nOSNDS Station {0}: Attempting to load configuration file'.format(settings.station)) if settings.debug == True else None

	try:

		with open(station_configuration) as config: 
			
			try:

				settings.config = json.load(config)

			except:

				print('\nOSNDS Station {0}: There was an error parsing the configuration file'.format(settings.station)) if settings.debug == True else None

				kill = True

				return kill

		print('\nOSNDS Station {0}: The configuration file has been loaded'.format(settings.station)) if settings.debug == True else None
		

	except FileNotFoundError:
		
		print('\nOSNDS Station {0}: There appears to be no configuration file for\n                 this station. Please ensure that the following\n                 file exists:\n\n                 {1}'.format(settings.station, station_configuration))

		kill = True

		return kill

	kill = k2_file.file_handler(settings.station, settings.config)

	if kill == True:

		return kill 

	try:
		
		influx.client = DataFrameClient(host = settings.config['influx']['host'],    # (default) storage.osnds.net
								port = settings.config['influx']['port'],            # (default) 8086
								database = settings.config['influx']['database'],    # (default) livestream-test
								username = settings.config['influx']['username'],    # (default) <redacted>
								password = settings.config['influx']['password'],)   # (default) <redacted>

	except:
	
		print('\nOSNDS Station {0}: There was an error initializing the client\n                 Please check your connection settings'.format(settings.station))

		kill = True

		return kill

	# InfluxDB (1.x) queries follow a similar style to SQL; "select * from <db>", etc.
	# 
	# Influx will perform math for you as well. In this instance, InfluxDB is being asked to return (3) separate values:
	#     - Median of the X component over the past (2) minutes
	#     - Median of the Y component over the past (2) minutes
	#     - Median of the Z component over the past (2) minutes
	# 
	# Since InfluxDB uses timestamps for its unique IDs, you have to include the time range "where time > now()-2m".
	#     now() = current time in ns since epoch
	# 
	# The "topic" is specific to how OSNDS receives MQTT streams, in this case, its how we specify which OSNDS station we wish to pull from

	data.last_event_id = settings.config['k2s0']['last_event_id']
	settings.trigger_cooldown = settings.config['k2s0']['trigger_cooldown_s']*10**9
	settings.debug = settings.config['k2s0']['debug']
	
	influx.query_median = str("SELECT median(x), median(y), median(z) FROM livestream_test.five_days_only.osnds_station_1 WHERE time > now()-2m AND data='seismic';".format(settings.station,settings.config['k2s0']['sensor']))
	print(influx.query_median)
	# query, compute, and store the median values (based upon the query above)
	kill = pull_medianValues()    

	# Influx will perform math for you. In this instance, InfluxDB is being asked to subtract the median values of X, Y, and Z from all future data pulls (respectively)
	# All three components are then added together. This is to ensure that k2so triggers off of an anomaly in any component

	influx.query_data = str("SELECT (x-({0})) + (y-({1})) + (z-({2})) FROM livestream_test.five_days_only.osnds_station_1 WHERE time > now()-{3}s AND data='seismic' fill(previous);".format(str(data.med_x),str(data.med_y),str(data.med_z),str(settings.config['k2s0']['time_window_s']),settings.station,settings.config['k2s0']['sensor']))
	print(influx.query_median)
	#k2s0_arguments = (settings.station, influx.query_data, influx.client, settings.config)

	if kill == True:
		
		return kill

	else:

		print('\nOSNDS Station {0}: K-2S0 has been successfully configured'.format(settings.station)) if settings.debug == True else None
	
		return kill




def pull_medianValues():

	if data.med_x == None:

		# this try statement catches an error where the Influx DataFrameClient is unable to run the specified query
		# this error will only occur if there is an issue with the client settings or query syntax
		try:

			response = influx.client.query(influx.query_median)     # send the initialization query to InfluxDB
			print(response)
			
			# this try statement catches an error where the Influx DataFrameClient successfully connected to the database but there was no data to pull
			# this happens when the user points k2s0 to a station that either doesnt exist or is currently offline
			try:
				
				median_values = response['osnds_station_1']              # get the "livestream" dataframe from the returned list of dataframes "response"

				data.med_x = median_values.loc[:,'median'][0]       # get the median of X from the dataframe
				data.med_y = median_values.loc[:,'median_1'][0]     # get the median of Y from the dataframe
				data.med_z = median_values.loc[:,'median_2'][0]     # get the median of Z from the dataframe

				kill = False

			except:

				print('\nOSNDS Station {0}: The station appears to be offline at the moment'.format(settings.station))

				kill = True
				return kill

		except:

			print('\nOSNDS Station {0}: The Influx client experienced an error'.format(settings.station))

			kill = True
			return kill

	else:

		# store the current median values in temporary variables
		previous_med_x = data.med_x
		previous_med_y = data.med_y
		previous_med_z = data.med_z

		# get new median values
		response = influx.client.query(influx.query_median)     # send the initialization query to InfluxDB
		median_values = response['osnds_station_1']                  # get the "livestream" dataframe from the returned list of dataframes "response"

		# store new median values in a temporary variables
		current_med_x = median_values.loc[:,'median'][0]        # get the median of X from the dataframe
		current_med_y = median_values.loc[:,'median_1'][0]      # get the median of Y from the dataframe
		current_med_z = median_values.loc[:,'median_2'][0]      # get the median of Z from the dataframe

		# average the current and previous median values
		data.med_x = (current_med_x + previous_med_x) / 2       # return the average of the current and previous median values for X
		data.med_y = (current_med_y + previous_med_y) / 2       # return the average of the current and previous median values for Y
		data.med_z = (current_med_z + previous_med_z) / 2       # return the average of the current and previous median values for Z
		
		print('\nOSNDS Station {0}: Updated median offset values are...\n \n    X = {1} m/s2\n    Y = {2} m/s2\n    Z = {3} m/s2'.format(settings.station, data.med_x, data.med_y, data.med_z)) if settings.debug == True else None

	return



def pull_fromInflux():
	
	response = influx.client.query(influx.query_data)            # send the initialization query to InfluxDB

	try:
		signal = response['osnds_station_1']                          # get the "livestream" dataframe from the returned list of dataframes "response"
	
		if signal.isnull().values.any() == False:                # validate that there are no "NA" values within the dataframe

			signal.index = pd.to_datetime(signal.index, format='%Y-%m-%d %H:%M:%S.%f%z', unit='ns')  # convert the <string> datetime to a datetime type
			signal.index = signal.index.astype('datetime64[ns]')                                     # force the datetime type to be "datetime64[ns]"

		else:

			print('\nOSNDS Station {0}: The latest pull from InfluxDB returned null values'.format(settings.station))
		
		if len(data.waveform) < settings.config['k2s0']['buffer']:

			data.waveform = data.waveform.combine_first(signal)
			
			print('\nOSNDS Station {0}: Successfully pulled new data (Buffer: {1} %)'.format(settings.station,math.ceil((len(data.waveform['x_y_z'])/settings.config['k2s0']['buffer'])*100))) if settings.debug == True else None

		else:

			data.waveform = data.waveform.combine_first(signal)
			data.waveform = data.waveform.iloc[len(signal):]

			print('\nOSNDS Station {0}: Successfully pulled new data (Buffer: {1} %)'.format(settings.station,math.ceil((len(data.waveform['x_y_z'])/settings.config['k2s0']['buffer'])*100))) if settings.debug == True else None

		return

	except KeyError:

		print('\nOSNDS Station {0}: X - The station appears to be offline at the moment.'.format(settings.station))

		return



def filter_waveform():

	# Scipy's median filter applys a median filter to the input array using a local window-size given by "kernel_size". The array will automatically be zero-padded.
	# Median filters are a great way to reduce higher-frequency noise, but you should be mindful that they essentially serve as a low-pass filter with a low, gaussian roll-off factor. 

	if settings.debug == True:

		start = time.time()   # get start time

	
	if settings.config['filtering']['enabled'] and settings.config['filtering']['bandpass_filter']['enabled'] == True:
		
		sos = scipy.signal.butter(3, 4, 'hp', fs=settings.config['fft_processing']['sample_rate'], output='sos')
		filtered = scipy.signal.sosfilt(sos, data.waveform['filtered'])

		data.waveform['filtered'] = filtered


	if settings.config['filtering']['enabled'] and settings.config['filtering']['median_filter']['enabled'] == True:
		
		data.waveform['filtered'] = scipy.signal.medfilt(volume = data.waveform['filtered'],                                          # input 1D signal
														kernel_size = settings.config['filtering']['median_filter']['kernel_size'])    # (defualt) 3

	
	if settings.config['filtering']['enabled'] and settings.config['filtering']['wavelet_filter']['enabled'] == True:

		# Skimage's wavelet filter 
		sigma_est = estimate_sigma(	image = data.waveform['filtered'],     # in this case, we are treating our 1D signal array as an image with a depth of 1-pixel and a length of n-pixels
									multichannel=False)                     # color images are mutli-channeled (R, G, B) whereas black/white images (or in our case a 1D signal array) are single-channeled

		data.waveform['filtered'] = denoise_wavelet(
									image = data.waveform['filtered'],                                      # in this case, we are treating our 1D signal array as an image with a depth of 1-pixel and a length of n-pixels 
									sigma = sigma_est,                                                      # here we are incorporating the estimated sigma for the median-filtered signal
									wavelet = settings.config['filtering']['wavelet_filter']['wavelet'],    # 
									multichannel = False,                                                   # color images are mutli-channeled (R, G, B) whereas black/white images (or in our case a 1D signal array) are single-channeled
									rescale_sigma = True,                                                   # 
									method = settings.config['filtering']['wavelet_filter']['method'],      # 
									mode = settings.config['filtering']['wavelet_filter']['thresholding'])  # 
	

	if settings.config['plot_signal']['enabled'] == True:

		plt.plot(data.waveform['x_y_z'], label='Original Signal')
		plt.plot(data.waveform['filtered'], label='Filtered Signal')
		plt.xlabel('Time Window (UTC)')
		plt.ylabel(str(settings.config['plot_signal']['y_label']+" $"+settings.config['plot_signal']['y_units']+"$"))
		
		plt.title('Filtered Signal Output')
		plt.legend()

		plt.show(block=False)
		plt.pause(2)
		plt.close()

	
	if settings.debug == True:

		end = time.time()

	print('\nOSNDS Station {0}: {1} records filtered in {2} seconds'.format(settings.station, len(data.waveform.filtered), math.ceil((end-start)*10000)/10000)) if settings.debug == True else None

	return



def detect_anomalies():
	
	sample_rate = settings.config['fft_processing']['sample_rate']
	signal_length = len(data.waveform['filtered'])

	if settings.config['anomaly_detector'] == "tad":

		anomalies = tad.anomaly_detect_vec(	x=data.waveform['filtered'],                   # pass the combined X+Y+Z waveform to the to the anomaly detector
											alpha=.0001,                                   # only return points that are deemed be be anomalous with a 99.9% threshold of confidence
											period=math.ceil(signal_length/sample_rate),   # 20% of the length of the signal, rounded up to an integer
											direction="both",                              # look at both the positive and negative aspects of the signal 
											e_value=True,                                  # add an additional column to the anoms output containing the expected value
											plot=False)                                    # plot the seasonal and linear trends of the signal, as well as the residual (detrended) data

	if settings.config['anomaly_detector'] == "global_shed_grubbs":

		None

	if settings.config['anomaly_detector'] == "global_shed_grubbs":

		None


	print('\nOSNDS Station {0}: K-2S0 detected {1} anomalies'.format(settings.station, len(anomalies))) if settings.debug == True else None

	if len(anomalies) > settings.config['k2s0']['anomaly_threshold']:    # serves as a basic filter for random suprious "anomalies" that can arise from any of the detection algorithims

		print("I'm about to get crazy! Hold my beer...")
		data.waveform['anomalies'] = anomalies
		data.waveform['anomalies'] = data.waveform['anomalies'].notna()  # replaces NA values with boolean False, True values stay True

		error_handling()
		parse_anomalies()
		
		return

	else:

		data.waveform['anomalies'] = False  # ensures that (in the case of no anomalies) all 'anomalies' values are False
		data.waveform['id'] = NaN
		data.waveform['reported'] = NaN

		return


def error_handling():

	# ensures there is an 'id' column within the dataframe on the first run (this prevents errors downstream)
	#None if "id" in data.waveform else data.waveform['id'] = NaN

	# ensures there is a 'reported' column within the dataframe on the first run (this prevents errors downstream)
	#None if "reported" in data.waveform else data.waveform['reported'] = NaN

	# ensures there is a 'grafanaID' column within the dataframe on the first run (this prevents errors downstream)
	#None if "grafanaID" in data.waveform else data.waveform['grafanaID'] = NaN

	return



def parse_anomalies():

	for i, g in data.waveform.groupby([(data.waveform.anomalies != data.waveform.anomalies.shift()).cumsum()]):

		if g.anomalies.all() == True:
			
			# has this anomaly group already been reported to OSNDS?
			if data.waveform.loc[g.first_valid_index():g.last_valid_index(),'reported'].sum() > 0:

				pass

			else:

				# is this anomaly group part of the previous anomaly group?
				if (float(g.first_valid_index().value) <= (data.previous_valid_last_value + settings.trigger_cooldown)):

					data.waveform.loc[data.previous_valid_first_index:g.last_valid_index(),'id'] = int(data.last_event_id)
					data.previous_valid_last_index = g.last_valid_index()
					data.previous_valid_last_value = g.last_valid_index().value

				else:

					data.last_event_id = data.last_event_id + 1

					# saves the current unique event ID to a new column within the dataframe called "id" - this event id is only applied to the indexes
					# bounded by the groupby function (e.g. start index for group g = g.first_valid_index | ending index for group g = g.last_valid_index)
					data.waveform.loc[g.first_valid_index():g.last_valid_index(),'id'] = int(data.last_event_id)
					
					# store the timestamp of the first anomalous amplitude within the anomaly group
					data.previous_valid_first_index = g.first_valid_index()
					data.previous_valid_first_value = g.first_valid_index().value

					# store the timestamp of the last anomalous amplitude within the anomaly group
					data.previous_valid_last_index = g.last_valid_index()
					data.previous_valid_last_value = g.last_valid_index().value
	
	data.waveform['id'].fillna(0)

	return



def send_alert(alert_message):

	alert_payload = {}

	alert_url = "https://config.osnds.net/api/alerts"  # OSNDS API URL for alerts (see Node-Red or NiFi for message handling)

	if alert_message['status'] == 'new':

		alert_payload = {
			"station"	:	settings.station,           # which station the event occurred on
			"event_id"	:	alert_message['id'],    # unique event ID
			"start_ns"	:	alert_message['start_ns'],              # start time in nanoseconds since epoch
			"stop_ns"	:	alert_message['stop_ns'],                # stop time in nanoseconds since epoch
			"start_real":	alert_message['start_real'].strftime("%d-%b-%Y (%H:%M:%S.%f)-UTC"), 
			"message"	:	alert_message['status']                 # general event message (this is mostly a placeholder)
		}

	if alert_message['status'] == 'update':

		alert_payload = {
			"ID"		:	alert_message['grafanaID'],
			"stop_ns"	:	alert_message['stop_ns'],
			"message"	: 	alert_message['status'] 
		}
	
	if alert_message['status'] == 'stop':

		alert_payload = {
			"station"	:	settings.station,
			"event_id"	:	alert_message['id'],    # unique event ID
			"stop_ns"	:	alert_message['stop_ns'],
			"message"	: 	alert_message['status'],
			"stop_real":	alert_message['stop_real'].strftime("%d-%b-%Y (%H:%M:%S.%f)-UTC"), 
			"ID"		:	alert_message['grafanaID'],
		}
	
	try:

		print(alert_url, alert_payload)
		alert_post = requests.post(alert_url, json=alert_payload, timeout = 1)  # post message payload to the API URL and store the response
		
		print('\nOSNDS Station {0}: API POST returned with code ({1}) and repsonse ({2})'.format(settings.station, alert_post.status_code, alert_post.text)) if settings.debug == True else None
		
		if alert_post.status_code == 200:

			if alert_message['status'] == 'new':
			
				returnJSON = alert_post.text
				returnDict = json.loads(str(returnJSON))
				annotID = returnDict['id']
		
				return {alert_post.status_code, annotID}
			
			if alert_message['status'] == 'update':
			
				return alert_post.status_code
		
			print('\nOSNDS Station {0}: A new anomaly has been reported:\n    Event ID:        {1}\n    Start Time (ns): {2}\n    End Time (ns):   {3}'.format(settings.station, data.last_event_id, alert_message['start_ns'], alert_message['stop_ns'])) if settings.debug == True else None
		
		else:

			print('\nOSNDS Station {0}: A new anomaly has been detected but failed to be reported to OSNDS (Status Code: {1})'.format(settings.station, alert_post.status_code))
		
	except:

		print('\nOSNDS Station {0}: A new anomaly has been detected but failed to be reported to OSNDS - please check internet connection'.format(settings.station, alert_post.status_code))

	return 



def event_publisher():

	print('\nOSNDS Station {0}: Scanning for anomalies'.format(settings.station)) if settings.debug == True else None

	if data.waveform.empty:

		pass

	else:
		
		try:
			
			unique_event_numbers = data.waveform.id.unique()   # get unique values in events, i.e. null,1,2,3
		
		except KeyError:

			data.waveform['id'] = NaN
			return

		filtered_unique_event_numbers = unique_event_numbers[~pd.isna(unique_event_numbers)] #filter out the nulls

		for unique_event_number in filtered_unique_event_numbers: #for each number found in the list

			event_start_time_ns = data.waveform.loc[data.waveform.id==unique_event_number].first_valid_index() #first timestamp for that event number
			event_end_time_ns = data.waveform.loc[data.waveform.id==unique_event_number].last_valid_index() #last timestamp for that event number

			#if yes, see if the last entry is marked as reported

			subset_Unique_ID=data.waveform.loc[data.waveform.id==unique_event_number] # produces a list of all the ['reported'] values present
			subset_Unique_Reported_Values = subset_Unique_ID.reported.unique()	# condenses list to be one of each value present

			if True in subset_Unique_Reported_Values: # are any True values found in ['reported']?

				subset_Unique_Grafana_ID_Values = subset_Unique_ID.grafanaID.unique() # get unique grafanaIDs
				filtered_subset_Unique_Grafana_ID_Values = subset_Unique_Grafana_ID_Values[~pd.isna(subset_Unique_Grafana_ID_Values)] # exclude NaN's
				
				# **************This is the update block ************************
				if len(subset_Unique_Reported_Values)>1: # yes, but some in the list arent reported=true, this is an update situation

					if len(filtered_subset_Unique_Grafana_ID_Values) > 1:

						pass # an error has occured here, we have multiple ID's for the same event

					else:
						grafanIDviaDF = str(int(filtered_subset_Unique_Grafana_ID_Values[0])) # grab the first grafanaID

					# prepare payload for alert
					alert_message = {
						"start_ns"	:	None,
						"stop_ns"	:	event_end_time_ns.value,
						"grafanaID"	:	grafanIDviaDF,
						"status"	:	'update'
						}

					statusCode = send_alert(alert_message) # send payload to communication routine

					#check to see if status reports back 'updated'
					if statusCode == 200:
						# reported back 'update', so we update the dataframe with 'reported'=true and add the grafanaID to new entries
						data.waveform.loc[event_start_time_ns:event_end_time_ns, 'grafanaID']= grafanIDviaDF
						data.waveform.loc[event_start_time_ns:event_end_time_ns, 'reported'] = True
					
					else:
					
						pass # bad status return, the code loop will keep trying to update until it leaves the window
				
				# ***************This is the stop flag block ****************************	
				
				if len(subset_Unique_Reported_Values)==1: # no other entries are not reported in the list

					grafanIDviaDF = str(int(filtered_subset_Unique_Grafana_ID_Values[0])) # grab the first grafanaID
										
					if grafanIDviaDF not in DataStore.report_ID_buffer:

						end_current_time_window = data.waveform.last_valid_index() #get the most recent influx data point
						waiting_period_to_kill_event = settings.trigger_cooldown*3*10**-9
						timeSinceEvent = float((end_current_time_window - event_end_time_ns).total_seconds()) #how long between the end of event and most recent point
						
						# check to see if X time has passed since last value
						# if time has been exceeded
						cooldown_expired_bool = timeSinceEvent > waiting_period_to_kill_event  # has the trigger cooldown been exceeded
						if (cooldown_expired_bool):
							#print('expired')
							alert_message = {
								"start_ns"		:	event_start_time_ns.value,
								"stop_ns"		:	event_end_time_ns.value,
								"status"		:	'stop',
								"stop_real"		:	event_end_time_ns,
								"grafanaID"		:	grafanIDviaDF,
								"id"			:	unique_event_number								
								}
							# send finished flag here
							statusCode = send_alert(alert_message) # send payload to communication routine
							# if status = 200
								#stuff
							DataStore.report_ID_buffer.append(grafanIDviaDF)
					
						else:

							pass

					else:
					
						pass

			# ********************** this is the new block *******************

			else: # No true found in ['reported']
				# not reported on, new report, send this alert up
				alert_message = {
					"start_ns"		:	event_start_time_ns.value,
					"stop_ns"		:	event_end_time_ns.value,
					"status"		:	'new',
					"start_real"	:	event_start_time_ns,
					"id"			:	unique_event_number
					 }

				statusCode, grafanaID = send_alert(alert_message)

				if statusCode == 200: #check to ensure good info transmission to grafana
					# mark ID number
					data.waveform.loc[event_start_time_ns:event_end_time_ns, 'grafanaID']= int(grafanaID)
					# mark it as reported=true
					data.waveform.loc[event_start_time_ns:event_end_time_ns, 'reported'] = True
				
				else:
				
					pass # bad status return, the code loop will keep trying to update until it leaves the window

	return



def k2so_detector():

	pull_fromInflux()
	
	data.waveform['filtered'] = data.waveform['x_y_z']

	filter_waveform() if settings.config['filtering']['enabled'] == True else None
	
	detect_anomalies()

	return



def run(station):

	settings.station = station

	kill = initialize_k2so()

	if kill != True:
			
		if settings.config['k2s0']['debug'] == True:

			settings.debug = True
			print('\nOSNDS Station {0}: K-2S0 is running normally on Process ID: {1}'.format(station,os.getpid()))
		
		else:

			print('\nOSNDS Station {0}: K-2S0 is running normally on Process ID: {1}'.format(station,os.getpid()))

		time.sleep(0.5)

		task.LoopingCall(pull_medianValues).start(settings.config['k2s0']['median_update_rate_m']*60)
		task.LoopingCall(event_publisher).start(settings.trigger_cooldown*10**-9)
		task.LoopingCall(k2so_detector).start(settings.config['k2s0']['data_pull_rate_s'])
		reactor.run()
	
	else:

		print('\nOSNDS Station {0}: K-2S0 will stop monitoring this station'.format(settings.station))

		pass