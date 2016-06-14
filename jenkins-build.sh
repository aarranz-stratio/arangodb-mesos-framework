#!/bin/sh

set -e
docker rm -f "$JOB_NAME"-builder || true
docker run -d --name "$JOB_NAME"-builder -v $WORKSPACE:/arangodb-mesos-framework m0ppers/mesos-builder-ubuntu-wily:0.28.1 tail -f /var/log/lastlog

docker exec -i "$JOB_NAME"-builder bash <<MESOSBUILD
cd /arangodb-mesos-framework
apt-get update
apt-get -y install libgoogle-glog-dev libmicrohttpd-dev cmake libunwind-dev
cd /arangodb-mesos-framework
rm -rf build
mkdir -p build
cd build
ln -s /mesos-0.27.2 mesos
cmake ../
make -j8
MESOSBUILD
docker build -t arangodb/arangodb-mesos-framework:3.0 .
docker inspect arangodb/arangodb-mesos-framework:3.0
docker rm -f "$JOB_NAME"-builder
docker save arangodb/arangodb-mesos-framework:3.0 > arangodb-mesos-framework_3.0.tar
rm -f arangodb-mesos-framework_3.0.tar.gz
gzip arangodb-mesos-framework_3.0.tar
