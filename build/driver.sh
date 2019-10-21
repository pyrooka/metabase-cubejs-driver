# Create a container.
docker run --name metabase-driver-builder -v $(dirname $(pwd)):/driver/metabase-cubejs-driver metabase-driver-builder /bin/sh -c "lein clean; DEBUG=1 LEIN_SNAPSHOTS_IN_RELEASE=true lein uberjar"

# Remove the container.
docker rm metabase-driver-builder