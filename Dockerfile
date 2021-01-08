# set base image (host OS)
FROM python:3.8

RUN apt-get update -yqq 
RUN apt-get upgrade -yqq 
RUN apt-get install -yqq nano 
RUN apt-get install -yqq vim 
RUN apt-get install -yqq curl 
RUN apt-get install -yqq wget 
RUN apt-get install net-tools

# set the working directory in the container
WORKDIR /code

# copy the dependencies file to the working directory
COPY requirements.txt .

# install dependencies
RUN pip install -r requirements.txt

# copy the content of the local src directory to the working directory
COPY . .

EXPOSE 5000

# command to run on container start
CMD [ "python", "./index.py" ]