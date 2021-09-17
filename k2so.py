# 
# K2S-O: Real-time, multi-processed, multi-threaded time-series anomaly detection
# 
# K2so pulls data from a time-series database on a pre-defined timing schedule, adding to an in-memory buffer each time.
# Median and wavelet filters are applied against the pulled datastreams to reduce noise. The resultant signal is then detrended
# using STLOESS detrending and then ran through a Season Hybrid Extreme Studentized Deviate to assess the waveform for statistical
# anomalies. Those anomalies are returned, paired with the original signal (as well as the filtered signal for cross-referencing) and
# then reported to OSNDS's alerting API. Logic has been added to group nomalies together into "events"; this is not an association approach,
# but rather a nearby clustering method prescribed by an end-user's set "reset time window".  
# 
# This python script was collaboratively written by James Stroup, Ed Robbins, and Samuel Stevens based upon the excellent original work 
# (written in R) by Ed Robbins which leveraged the foundational work of Twitter's Reasearch Team (as well as numerous open-source packages).
# Please see the end of this file for a full list of usage credits.
# 
# Usage instructions:
# 
# K-2SO is ultimately called and executed by this script. You can call it by running the following comamand from within the project folder:
# 
#     python k2so.py -s [stations]
#
# wherein the "-s" is an argument flag for "stations", as in which stations you would like k2so to monitor against.
# You must follow this flag by each station's ID, separated by spaces (single-word alphaumerics are accepted). For example:
# 
#     python k2so.py -s 1 2 3 4 X1 X12
# 
# We will soon be adding a "-d" argument to the scripts execution that will force K2SO to operate in a DEBUG MODE. This mode will provide the
# end user with a log of K-2SOs output labed by Station ID. This feature is still in work. 
#


import sys, os, time, argparse
from multiprocessing import Pool

#sys.path.insert(0, r'/anomaly_detection/k2s0_python/src')
from src import logic as k2

# This function runs separate instances of k2so for each identified sensor specified by the command line 
def k2so(station):
	#station, debug = arguments  # first attempt to tuple/untuple arguments through a mapped process - may/may not work OOB
	k2.run(station)
	#k2_min.run(station)
	return


# The main start-up script for K-2SO. This function retrieves the arguments passed to it by the user and then maps out k2so to each sensor
if __name__ == "__main__":

	os.system('cls' if os.name == 'nt' else 'clear')
	
	parser = argparse.ArgumentParser(prog="K-2SO", 
									description=str("K-2SO is a sensor-agnostic real-time event (or anomaly) "+ 
									"detector developed by AFTAC/SI. This algorithim performs STL detrending "+
									"and S-H-ESD anomaly detection on stations the user identifies. NOTE: K2SO "+
									"is multi-processed, thread-safe, and (as of right now) requires the OSNDS "
									"platform for use."))

	# "-s" is an argument flag for "stations", e.g. python k2so.py -s 1 2 3 4 X1 X12
	parser.add_argument('-s', 
						action='store', 
						help='list of stations K-2SO should run against (separated by spaces) e.g. 1 2 3 X1', 
						nargs='+', 
						required=True, 
						metavar='')
	
	# "-d" forces each station to output its logs to the command line for ease of development
	parser.add_argument('-d', 
						action='store', 
						help='boolean input determining whether K-2SO should run in debug mode - e.g. t or f', 
						nargs='+', 
						required=False, 
						metavar='')

	args, left = parser.parse_known_args()

	stations = args.s
	debug_mode = args.d

	debug_bool = ["t", "T", "true", "True", "TRUE", "y", "Y", "yes", "Yes", "YES"]

	

	print(r"""
 ___  __                     _______  ________  ________     
|\  \|\  \                  /  ___  \|\   ____\|\   __  \    
\ \  \/  /|_  ____________ /__/|_/  /\ \  \___|\ \  \|\  \   
 \ \   ___  \|\____________\__|//  / /\ \_____  \ \  \\\  \  
  \ \  \\ \  \|____________|   /  /_/__\|____|\  \ \  \\\  \ 
   \ \__\\ \__\               |\________\____\_\  \ \_______\
    \|__| \|__|                \|_______|\_________\|_______|
                                        \|_________|         """)

	print("\nK-2S0 was collaboratively written by the great folks over in\nAFTAC/SI:\n")
	print("    - James Stroup, R&D Portfolio Manager (AI/ML)")
	print("    - Edmund Robbins, PhD Candidate (Applied Mathematics)")
	print("    - Capt Samuel Stevens, Deputy Branch Chief (Partnering)")

	#if debug_mode[0] in debug_bool:

	#	print("\nK2S0 is now running in DEBUG MODE on the following OSNDS Stations and Process IDs:")
	#	debug_mode = True
	
	#else:
	
	debug_mode = False
	
	print("\nYou may saftely quit K2SO at anytime by pressing [CTRL] + [C]")
	#print("\n+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
	
	arguments = (stations, debug_mode)

	try:

		print("\n+++++++++++++++++++++[All Stations Log]++++++++++++++++++++++++")

		with Pool(processes=len(stations)) as p:
			p.map(k2so, stations)  # need to add logic that passes DEBUG state the k2_helper

	except KeyboardInterrupt as e:

		print("\n+++++++++++++++++++++[End of K-2S0 Log]++++++++++++++++++++++++")
		print("\n[All Stations] Preparing to shut down K2S0...")
		time.sleep(.5)

		print("[All Stations] Closing multi-processing pool...")
		p.terminate()
		time.sleep(.5)

		print("[All Stations] Cleaning up threading...")
		p.join()
		time.sleep(.5)

		print("[All Stations] Exiting K2S0...")
		time.sleep(.7)
		
		#os.system('cls' if os.name == 'nt' else 'clear')
		sys.exit()
