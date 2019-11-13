# Build the driver again.
../build/driver.sh &&

# Copy our fresh new driver.
./copy.sh &&

# Run the containers.
docker-compose up