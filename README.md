# OSNDS-K2SO
 Dockerized K2SO
## Build command
 This image will build based on the dockerfile, which utilizes the requirements file for its package dependencies.

docker build --tag=osnds-k2so .

## Run command
 This image maintains the capability to utilize command line arguments, such as station numbers.  As such the command to create and run the container is required to have a station number appended to the end:

docker run --privileged -d -it --rm osnds-k2so 1

Or in the case of multiple stations

docker run --privileged -d -it --rm osnds-k2so 1 2

This run command will auto delete the container at the end of the run.  If you would like to have a full console debug level logging session running, simply omit the '-d' argument.

