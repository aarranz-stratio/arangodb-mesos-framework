#/bin/sh

if [ $# -lt 2 ]; then
  echo "$0 <docker-image-name> <build-tag>"
  echo "Example: $0 arangodb/arangodb-mesos-framework 3.0-build2"
  exit 99
fi

git remote get-url origin > mesos-scripts/BUILDTAG
git rev-parse HEAD >> mesos-scripts/BUILDTAG
echo $3 >> mesos-scripts/BUILDTAG
git tag -s $2

docker build -t $1:$2 .

echo "Git tag created: $2"
echo "Docker image created: $1:$2"
echo ""
echo "To finish the release:"
echo "docker push $1:$2 && git push --follow-tags"
