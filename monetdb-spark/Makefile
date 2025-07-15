
# Run 'make' or 'make build' to build the jar in target/.
build:
	./mvnw package -DskipTests

# Run 'make test' to test against the default database (testspark).
# To override the database, run
# 'make test TESTDB=demo' or 'make test TESTDB=jdbc:monetdb:///demo'.
test:
	./mvnw test $(if $(TESTDB),-Dtest.db=$(TESTDB),)

clean:
	./mvnw clean