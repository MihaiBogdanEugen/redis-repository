## help: Prints this help message
help:
	@echo "Usage: \n"
	@sed -n 's/^##//p' ${MAKEFILE_LIST} | column -t -s ':' |  sed -e 's/^/ /'

## clean: Deletes the project build directory
clean:
	./gradlew clean

## build: Performs a clean build without running tests
build: clean
	./gradlew --parallel build -x test

## test: Cleans the project and runs a full build
test: clean
	./gradlew test

## dependency-updates: Checks for dependency updates
dependency-updates:
	./gradlew dependencyUpdates

## local-publish: Publishes the package in the local repository
local-publish: test check-gpg-passphrase check-gpg-private-key
	./gradlew publishToMavenLocal

## check-gpg-private-key: Ensure the GPG_PRIVATE_KEY environment variable is defined
check-gpg-private-key:
ifndef GPG_PRIVATE_KEY
	$(error "GPG_PRIVATE_KEY is undefined")
endif

## check-gpg-passphrase: Ensures the GPG_PASSPHRASE environment variable is defined
check-gpg-passphrase:
ifndef GPG_PASSPHRASE
	$(error "GPG_PASSPHRASE is undefined")
endif

.PHONY: help clean build test dependency-updates local-publish check-gpg-private-key check-gpg-passphrase
