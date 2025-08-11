# top level Makefile
#
# For now we simply forward to targets in ./monetdb-spark/Makefile
build:
	make -C monetdb-spark build
test:
	make -C monetdb-spark test
clean:
	make -C monetdb-spark clean
