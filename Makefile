## help: Prints this help message
help:
	@echo "Usage: \n"
	@sed -n 's/^##//p' ${MAKEFILE_LIST} | column -t -s ':' |  sed -e 's/^/ /'

## clean: Deletes the project build directory
clean:
	./gradlew clean

## build: Performs a clean build without running tests
build: clean
	./gradlew build -x test

## test: Cleans the project and runs a full build
test: clean
	./gradlew test

## dependency-updates: Checks for dependency updates
dependency-updates:
	./gradlew dependencyUpdates

.PHONY: help clean build test dependency-updates
