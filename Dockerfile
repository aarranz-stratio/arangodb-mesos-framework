FROM ubuntu:15.10
MAINTAINER Frank Celler <info@arangodb.com>

RUN apt-get -y -qq --force-yes update
RUN apt-get install -y -qq libgoogle-glog0v5 libapr1 libsvn1 libboost-regex1.58.0 libmicrohttpd10 libcurl3-nss

# add mesos framework files
ADD ./mesos-scripts /mesos

# add the arangodb framework
ADD ./bin/arangodb-framework /mesos/arangodb-framework
ADD ./assets /mesos/assets

# start script
ENTRYPOINT ["/mesos/framework.sh"]
