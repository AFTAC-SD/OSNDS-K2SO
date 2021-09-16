# syntax=docker/dockerfile:1

#FROM python:3.8-slim-buster
FROM ubuntu:20.04
# FROM nvcr.io/nvidia/l4t-base:r32.6.1
ENV PATH="/root/miniconda3/bin:${PATH}"
ARG PATH="/root/miniconda3/bin:${PATH}"
ENV https_proxy=http://10.150.206.21:8080
ENV http_proxy=http://10.150.206.21:8080

RUN touch /etc/apt/apt.conf
RUN echo "Acquire::http::Proxy \"http://10.150.206.21:8080\";" > /etc/apt/apt.conf
# RUN export http_proxy=http://10.150.206.21:8080
# RUN export https_proxy=http://10.150.206.21:8080
RUN apt-get update
WORKDIR /app
RUN apt-get install -y vim
RUN apt-get install -y curl
RUN curl -LJO https://repo.anaconda.com/miniconda/Miniconda3-py38_4.10.3-Linux-x86_64.sh
RUN bash Miniconda3-py38_4.10.3-Linux-x86_64.sh -b
RUN rm -f Miniconda3-latest-Linux-x86_64.sh 
COPY requirements.txt requirements.txt
RUN conda install pip
RUN apt-get install -y python-dev
RUN apt-get install -y python3-dev
RUN apt install -y build-essential
RUN apt-get install -y libssl-dev
RUN pip3 install service_identity
RUN conda install -r requirements.txt
RUN apt-get -y update
RUN apt-get -y upgrade
# COPY functions.py functions.py
COPY k2so.py k2so.py
COPY ./k2s0_tad ./k2s0_tad
COPY ./logic_files ./logic_files
COPY ./configuration_files ./configuration_files
RUN pip3 install /k2s0_tad
# CMD [ "python3", "k2so.py", "-s", "1" ]
CMD echo "Test"