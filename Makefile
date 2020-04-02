TESTENV?=environments/test

## stop: Stops running test environment containers
.PHONY: stop
stop:
	@cd ${TESTENV} && docker-compose stop

## start: Starts a local test environment
.PHONY: start
start: stop build
	@mkdir -p ${TESTENV}/driver
	@cp cubejs.metabase-driver.jar ${TESTENV}/driver
	@cd ${TESTENV} && docker-compose up

## up: Starts the local test environment but withouth compiling the driver (just a docker-compose up)
.PHONY: up
up: stop
	@cd ${TESTENV} && docker-compose up

## docker: Builds the docker images for the driver building and the testing
.PHONY: docker
docker:
	@echo "Building metabase-driver-builder image..."
	@docker build -t metabase-driver-builder environments/build
	@echo "Building cubejs-metabackend image..."
	@docker build -t cubejs-metabackend ${TESTENV}/cubejs

## build: Builds the driver
.PHONY: build
build:
	@rm -rf target cubejs.metabase-driver.jar
	@docker run --rm -v $(shell pwd):/driver/metabase-cubejs-driver metabase-driver-builder /bin/sh -c "lein clean; DEBUG=1 LEIN_SNAPSHOTS_IN_RELEASE=true lein uberjar"
	@cp target/uberjar/cubejs.metabase-driver.jar ./

## repl: Starts a local REPL server for development
repl:
	docker run -it --rm -p 5555:5555 -v $(shell pwd):/driver/metabase-cubejs-driver metabase-driver-builder /bin/sh -c "lein repl :start :host 0.0.0.0 :port 5555"

## clean: Cleanups your workplace
.PHONY: clean
clean:
	@echo "Removing builds..."
	@rm -rf target
	@echo "Removing docker containers..."
	@cd ${TESTENV} && docker-compose rm -s -f
	@echo "Removing docker images..."
	@docker rmi metabase-driver-builder cubejs-metabackend

.PHONY: help
## help: Prints this help message
help:
	@echo "Usage: \n"
	@sed -n 's/^##//p' ${MAKEFILE_LIST} | column -t -s ':' |  sed -e 's/^/ /'