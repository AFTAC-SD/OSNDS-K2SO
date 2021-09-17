
import pywt

def file_handler(station, config):

	kill = False

	file_errors_basic = []
	file_errors_logic = []
	file_warnings = []
	wavelets = []
	available_wavelets = pywt.wavelist()
	wavelet_method = ['BayesShrink','VisuShrink']
	wavelet_thresholding = ['hard','soft']

	# check program operation section of the file
	None if isinstance(config['k2s0']['debug'], bool) else file_errors_basic.append('[k2s0/debug] must be a boolean value (true or false, no quotations)')
	None if isinstance(config['k2s0']['time_window_s'], int) and config['k2s0']['time_window_s'] > 0 else file_errors_basic.append('[k2s0/time_window_s] must be an integer greater than 0')
	None if isinstance(config['k2s0']['data_pull_rate_s'], int) and config['k2s0']['data_pull_rate_s'] > 0 else file_errors_basic.append('[k2s0/data_pull_rate_s] must be an integer greater than 0')
	None if isinstance(config['k2s0']['anomaly_threshold'], int) and config['k2s0']['anomaly_threshold'] > 0 else file_errors_basic.append('[k2s0/anomaly_threshold] must be an integer greater than 0')
	None if isinstance(config['k2s0']['buffer'], int) and config['k2s0']['buffer'] > 0 else file_errors_basic.append('[k2s0/buffer] must be an integer greater than 0')
	None if isinstance(config['k2s0']['trigger_cooldown_s'], int) and config['k2s0']['trigger_cooldown_s'] > 0 else file_errors_basic.append('[k2s0/trigger_cooldown_s] must be an integer greater than 0')
	None if isinstance(config['k2s0']['median_update_rate_m'], int) and config['k2s0']['median_update_rate_m'] > 0 else file_errors_basic.append('[k2s0/median_update_rate_m] must be an integer greater than 0')
	None if isinstance(config['k2s0']['sensor'], str) else file_errors_basic.append('[k2s0/sensor] must be a standard string (in quotations)')
	None if isinstance(config['k2s0']['last_event_id'], int) and config['k2s0']['last_event_id'] >= 0 else file_errors_basic.append('[k2s0/last_event_id] must not be a negative number')

	# check influx client section of the file
	None if isinstance(config['influx']['host'], str) else file_errors_basic.append('[influx/host] must be a standard string (in quotations)')
	None if isinstance(config['influx']['port'], int) and config['influx']['port'] > 0 else file_errors_basic.append('[influx/port] must be an integer greater than 0')
	None if isinstance(config['influx']['database'], str) else file_errors_basic.append('[influx/database] must be a standard string (in quotations)')
	None if isinstance(config['influx']['username'], str) else file_errors_basic.append('[influx/username] must be a standard string (in quotations)')
	None if isinstance(config['influx']['password'], str) else file_errors_basic.append('[influx/password] must be a standard string (in quotations)')

	# check user features section of the file
	None if isinstance(config['plot_signal']['enabled'], bool) else file_errors_basic.append('[plot_signal/enabled] must be a boolean value (true or false, no quotations)')
	None if isinstance(config['fft_processing']['enabled'], bool) else file_errors_basic.append('[fft_processing/enabled] must be a boolean value (true or false, no quotations)')
	None if isinstance(config['filtering']['enabled'], bool) else file_errors_basic.append('[filtering/enabled] must be a boolean value (true or false, no quotations)')

	# check filtering options
	if config['filtering']['enabled'] == True:

		None if isinstance(config['filtering']['bandpass_filter']['enabled'], bool) else file_errors_basic.append('[filtering/bandpass_filter/enabled] must be a boolean value (true or false, no quotations)')
		None if isinstance(config['filtering']['median_filter']['enabled'], bool) else file_errors_basic.append('[filtering/median_filter/enabled] must be a boolean value (true or false, no quotations)')
		None if isinstance(config['filtering']['wavelet_filter']['enabled'], bool) else file_errors_basic.append('[filtering/wavelet_filter/enabled] must be a boolean value (true or false, no quotations)')

		if config['filtering']['bandpass_filter']['enabled'] == True:
			None if isinstance(config['filtering']['bandpass_filter']['low_freq_hz'], int) and config['filtering']['bandpass_filter']['low_freq_hz'] > 0 else file_errors_basic.append('[filtering/bandpass_filter/low_freq_hz] must be an integer greater than 0')
			None if isinstance(config['filtering']['bandpass_filter']['high_freq_hz'], int) and config['filtering']['bandpass_filter']['high_freq_hz'] > 0 else file_errors_basic.append('[filtering/bandpass_filter/high_freq_hz] must be an integer greater than 0')
			None if isinstance(config['filtering']['bandpass_filter']['roll_off'], int) and config['filtering']['bandpass_filter']['roll_off'] > 0 else file_errors_basic.append('[filtering/bandpass_filter/roll_off] must be an integer greater than 0')

		if config['filtering']['median_filter']['enabled'] == True:
			None if isinstance(config['filtering']['median_filter']['enabled'], bool) else file_errors_basic.append('[filtering/median_filter/enabled] must be a boolean value (true or false, no quotations)')
			None if isinstance(config['filtering']['median_filter']['kernel_size'], int) and config['filtering']['median_filter']['kernel_size'] > 0 else file_errors_basic.append('[filtering/median_filter/kernel_size] must be an integer greater than 0')

		if config['filtering']['wavelet_filter']['enabled'] == True:
			None if isinstance(config['filtering']['wavelet_filter']['enabled'], bool) else file_errors_basic.append('[filtering/wavelet_filter/enabled] must be a boolean value (true or false, no quotations)')
			None if isinstance(config['filtering']['wavelet_filter']['wavelet'], str) else file_errors_basic.append('[filtering/wavelet_filter/wavelet] must be a standard string (in quotations)')
			None if isinstance(config['filtering']['wavelet_filter']['method'], str) else file_errors_basic.append('[filtering/wavelet_filter/method] must be a standard string (in quotations)')
			None if isinstance(config['filtering']['wavelet_filter']['thresholding'], str) else file_errors_basic.append('[filtering/wavelet_filter/thresholding] must be a standard string (in quotations)')

	# check plotting options
	if config['plot_signal']['enabled'] == True:

		None if isinstance(config['plot_signal']['save_plot'], bool) else file_errors_basic.append('[plot_signal/save_plot] must be a boolean value (true or false, no quotations)')
		None if isinstance(config['plot_signal']['save_plot_transparent'], bool) else file_errors_basic.append('[plot_signal/save_plot_transparent] must be a boolean value (true or false, no quotations)')
		None if isinstance(config['plot_signal']['y_label'], str) else file_errors_basic.append('[plot_signal/y_label] must be a standard string (in quotations)')
		None if isinstance(config['plot_signal']['y_units'], str) else file_errors_basic.append('[plot_signal/y_units] must be a standard string (in quotations)')

	# check fft processing options
	if config['fft_processing']['enabled'] == True:

		None if isinstance(config['fft_processing']['sample_rate'], int) and config['fft_processing']['sample_rate'] > 0 else file_errors_basic.append('[fft_processing/sample_rate] must be an integer greater than 0')
		None if isinstance(config['fft_processing']['fft_points'], int) and config['fft_processing']['fft_points'] > 0 else file_errors_basic.append('[fft_processing/fft_points] must be an integer greater than 0')
		None if isinstance(config['fft_processing']['percent_overlap'], int) and config['fft_processing']['percent_overlap'] > 0 else file_errors_basic.append('[fft_processing/percent_overlap] must be an integer greater than 0')
		None if isinstance(config['fft_processing']['pad_to'], int) and config['fft_processing']['pad_to'] > 0 else file_errors_basic.append('[fft_processing/pad_to] must be an integer greater than 0')
		None if isinstance(config['fft_processing']['scaling'], str) else file_errors_basic.append('[fft_processing/scaling] must be a standard string (in quotations)')
		None if isinstance(config['fft_processing']['detrending'], str) else file_errors_basic.append('[fft_processing/detrending] must be a standard string (in quotations)')

	if len(file_errors_basic) > 0:

		print('\nOSNDS Station {0}: Our file handler returned the following basic errors...\n'.format(station),*file_errors_basic, sep = "\n")

		kill = True

	# Check variable values for downstream logic errors
	None if config['k2s0']['time_window_s'] > config['k2s0']['data_pull_rate_s'] else file_errors_logic.append('[k2s0/time_window_s] must be greater than [k2s0/data_pull_rate_s]')
	None if config['k2s0']['buffer'] >= config['k2s0']['time_window_s'] * config['fft_processing']['sample_rate'] else file_errors_logic.append('[k2s0/buffer] must be greater than/equal to [k2s0/time_window_s] * [fft_processing/sample_rate]')
	
	if config['filtering']['enabled'] == True:

		if config['filtering']['bandpass_filter']['enabled'] == True:

			None if config['filtering']['bandpass_filter']['high_freq_hz'] > config['filtering']['bandpass_filter']['low_freq_hz'] else file_errors_logic.append('[filtering/bandpass_filter/high_freq_hz] must be greater than [filtering/bandpass_filter/low_freq_hz]')
			None if config['filtering']['bandpass_filter']['high_freq_hz'] - config['filtering']['bandpass_filter']['low_freq_hz'] > 2 * config['filtering']['bandpass_filter']['roll_off'] else file_errors_logic.append('[filtering/bandpass_filter/high_freq_hz] - [filtering/bandpass_filter/low_freq_hz] must be greater than 2 * [filtering/bandpass_filter/roll_off]')

		if config['filtering']['median_filter']['enabled'] == True:
			None if (config['filtering']['median_filter']['kernel_size'] % 2) != 0 else file_errors_logic.append('[filtering/median_filter/kernel_size] must be an odd integer')
			None if config['filtering']['median_filter']['kernel_size'] < 12 else file_warnings.append('[filtering/median_filter/kernel_size] a high kernel_size value will most likely cause K-2S0 to miss events')

		if config['filtering']['wavelet_filter']['enabled'] == True:
	
			for wavelet in available_wavelets:
				wavelets.append(wavelet.strip())

			desired_wavelet = config['filtering']['wavelet_filter']['wavelet']
			wavelets_str = '\n          '.join(map(str, wavelets)) 

			None if desired_wavelet in wavelets else file_errors_logic.append(str('[filtering/wavelet_filter/wavelet] needs to be one of the following wavelets:\n          {0}'.format(wavelets_str)))
			
			wavelet_method_str = '\n          '.join(map(str, wavelet_method)) 
			wavelet_thresholding_str = '\n          '.join(map(str, wavelet_thresholding)) 

			None if config['filtering']['wavelet_filter']['method'] in wavelet_method else file_errors_logic.append('[filtering/wavelet_filter/method] must be one of the following:\n          {0}'.format(wavelet_method_str))
			None if config['filtering']['wavelet_filter']['thresholding'] in wavelet_thresholding else file_errors_logic.append('[filtering/wavelet_filter/thresholding] must be one of the following:\n          {0}'.format(wavelet_thresholding_str))
	
	if len(file_errors_logic) > 0:

		print('\nOSNDS Station {0}: Our file handler returned the following logic errors...\n'.format(station),*file_errors_logic, sep = "\n")

		kill = True

	if len(file_warnings) > 0:

		print('\nOSNDS Station {0}: Our file handler returned the following warnings...\n'.format(station),*file_warnings, sep = "\n")

	return kill
