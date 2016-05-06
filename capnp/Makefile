CAPNP := $(patsubst %.capnp,%.capnp.go,$(filter-out go.capnp,$(wildcard *.capnp)))
CAPNPGO := $(wildcard *.capnp.go)

all: capnp

capnp: $(CAPNP)

clean:
	rm -f $(CAPNPGO)

%.capnp.go: %.capnp
	capnp compile -o go $<

.PHONY: all capnp clean
