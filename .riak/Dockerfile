#Riak - LeanKit

# Pull base image.
FROM ubuntu:14.04
MAINTAINER Jason Greathouse jason.greathouse@leankit.com

ENV RIAK_VERSION 2.1.1-1
ENV DEBIAN_FRONTEND noninteractive

# Update base image and install Riak
RUN apt-get update && \
    apt-get dist-upgrade -y && \
    apt-get install -y curl software-properties-common libpam0g-dev && \
    add-apt-repository -y ppa:webupd8team/java && \
    apt-get update && \
    echo debconf shared/accepted-oracle-license-v1-1 select true | debconf-set-selections && \
    apt-get install -y oracle-java7-installer

RUN curl -o /tmp/riak_${RIAK_VERSION}_amd64.deb http://s3.amazonaws.com/downloads.basho.com/riak/2.1/2.1.1/ubuntu/trusty/riak_${RIAK_VERSION}_amd64.deb && \
    dpkg -i /tmp/riak_${RIAK_VERSION}_amd64.deb

RUN apt-get clean && \
    rm -rf /var/lib/apt/lists/* 

# Add riak apt repo
#ADD .docker/etc/apt/sources.list.d /etc/apt/sources.list.d

# Install Riak
#RUN curl https://packagecloud.io/gpg.key | apt-key add - && \
#    apt-get install -y riak=${RIAK_VERSION} && \

# Riak config and ulimts
ADD .docker/etc/riak /etc/riak
ADD .docker/etc/security/limits.d /etc/security/limits.d

# Define mount points.
VOLUME ["/data"]

# Add files.
ADD .docker/usr/local/bin/start-app.sh /usr/local/bin/

# Define default command.
CMD ["/usr/local/bin/start-app.sh"]

# Ports.
# tcp - pb_port
EXPOSE 8087
# http - web port
EXPOSE 8098
# https - web port
EXPOSE 8099
