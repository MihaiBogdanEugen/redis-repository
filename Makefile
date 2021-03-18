## help: Prints this help message
help:
	@echo "Usage: \n"
	@sed -n 's/^##//p' ${MAKEFILE_LIST} | column -t -s ':' |  sed -e 's/^/ /'

## verify: Run any checks on results of integration tests to ensure quality criteria are met
verify:
	./mvnw clean verify

## package: Take the compiled code and package it in its distributable format
package:
	./mvnw -B clean package --file pom.xml

## check-dependency-updates: Check all the dependencies used in your project and display a list of those dependencies with newer versions available
check-dependency-updates:
	./mvnw versions:display-dependency-updates

## check-pom-xml: Check the contents of a POM file against the rules of maven central
check-pom-xml:
	./mvnw enforcer:enforce

## local-deploy: Deploy locally a snapshot version
local-deploy:
	./mvnw -Ppublication,local-deploy -Dlocal.repository.path=$(shell pwd)/target/repository deploy

.PHONY: help verify package check-dependency-updates check-pom-xml local-deploy
