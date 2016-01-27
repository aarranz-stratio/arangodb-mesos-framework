#/bin/sh

if [ $# -lt  3 ]; then
  echo "$0 <docker-image-name> <mesosphere-release-line> <build-tag>"
  echo "Example: $0 arangodb/arangodb-mesos-framework mesosphere-V2 build7"
  exit 99
fi

git remote get-url origin > mesos-scripts/BUILDTAG
git rev-parse HEAD >> mesos-scripts/BUILDTAG
echo $3 >> mesos-scripts/BUILDTAG
git tag -s $3

docker build -t $1:$2 .
docker tag $1:$2 $1:$3

echo "Git tag created: $3"
echo "Docker images created: $1:$2, $1:$3"
echo ""
echo "To finish the release:"
echo "docker push $1:$2 && docker push $1:$3 && git push --follow-tags"
