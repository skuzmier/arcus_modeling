#Base docker image for the project

#create a ubuntu 20.04 image
FROM ubuntu:22.04

RUN apt-get update && apt-get upgrade
RUN apt-get install -y build-essential zlib1g-dev libncurses5-dev libgdbm-dev libssl-dev \
    libreadline-dev libffi-dev wget libbz2-dev libsqlite3-dev
RUN mkdir /python && cd /python
RUN wget https://www.python.org/ftp/python/3.11.1/Python-3.11.1.tgz
RUN tar -zxvf Python-3.11.1.tgz
RUN cd Python-3.11.1 && ls -lhR && ./configure --enable-optimizations && make install



#install pip
RUN apt-get install -y python3-pip
#upgrade pip 
RUN pip3 install --upgrade pip

#switch to bash shell
SHELL ["/bin/bash", "-c"]

#Create a virtual environment to prevent conflicts with system python
RUN python3.11 -m venv /virts_env
ENV PATH="/virts_env/bin:$PATH"
RUN source activate virts_env

#install git
RUN apt-get install -y git

#git clone the project using the token from the environment variable
#ARG GITHUB_TOKEN

#RUN git clone https://$GITHUB_TOKEN:@github.com/skuzmier/ercot_virts.git

#set the working directory
WORKDIR /ercot_virts

# copy the requirements file to the working directory
COPY requirements.txt /ercot_virts
#Copy secrets from the docker_secrets folder
COPY ./docker_secrets/docker_secrets.sh .

# Create the SSH directory and give it the right permissions
RUN mkdir /root/.ssh/
RUN chmod 0700 /root/.ssh

#Add the ssh key for the github repository
COPY ./docker_secrets/id_rsa_arcus /root/.ssh/id_rsa
COPY ./docker_secrets/id_rsa_arcus.pub /root/.ssh/id_rsa.pub
RUN chmod 600 /root/.ssh/id_rsa
RUN chmod 600 /root/.ssh/id_rsa.pub
CMD ["source docker_secrets.sh"]

RUN git clone git@github.com:skuzmier/ercot_virts.git


#install the requirements from the ercot_virts project
RUN pip install --upgrade pip
RUN pip install pandas
RUN cd /ercot_virts
RUN pip install -r requirements.txt

