PACKAGE_BASE=$(dir $(shell go list -e ./))

all: goshawkdb goshawkdb.test

clean:
	rm -f goshawkdb goshawkdb.test

goshawkdb:
	go build

goshawkdb.test:
	$(eval COVERAGE_MODULES := $(shell (go list .; go list -f '{{join .Deps "\n"}}' . | grep "^$(PACKAGE_BASE)") | grep -v "/capnp" | paste -s -d, - ))
	go test -c -v -covermode atomic -coverpkg $(COVERAGE_MODULES)

.PHONY: all goshawkdb goshawkdb.test
