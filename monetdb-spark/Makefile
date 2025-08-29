
# Run 'make' or 'make build' to build the jar in target/.
build:
	./mvnw --batch-mode package -DskipTests

# Run 'make test' to test against the default database (testspark).
# To override the database, run
# 'make test TESTDB=demo' or 'make test TESTDB=jdbc:monetdb:///demo'.
test:
	./mvnw --batch-mode test $(MAVEN_OPTS) $(if $(TESTDB),-Dtest.db=$(TESTDB),)

clean:
	./mvnw --batch-mode clean
