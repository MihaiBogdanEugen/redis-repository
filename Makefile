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

## check-maven-central: Check the contents of a POM file against the rules of maven central
check-maven-central:
	./mvnw org.kordamp.maven:pomchecker-maven-plugin:1.1.0:check-maven-central

.PHONY: help verify package check-dependency-updates check-maven-central