FROM ubuntu:15.10
MAINTAINER Frank Celler <info@arangodb.com>

RUN apt-get -y -qq --force-yes update
RUN apt-get install -y -qq libgoogle-glog0v5 libapr1 libsvn1 libmicrohttpd10 libcurl3-nss

# add mesos framework files
ADD ./mesos-scripts /mesos

# add the arangodb framework
ADD ./build/bin/arangodb-mesos-framework /mesos/arangodb-framework
ADD ./assets /mesos/assets

# start script
ENTRYPOINT ["/mesos/framework.sh"]
