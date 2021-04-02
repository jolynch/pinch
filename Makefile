.PHONY: image install uninstall acceptance test

DESTDIR ?=
PREFIX  ?= /usr/local

image:
	docker build -t jolynch/pinch .
	docker build -t jolynch/pinch-server server

acceptance: image test

test:
	tests/fetch_data.sh
	tests/test_server.sh

install: image
	@install -v -d -m 755 $(DESTDIR)$(PREFIX)/bin
	@install -v pinch $(DESTDIR)$(PREFIX)/bin
	@install -v pinch-server $(DESTDIR)$(PREFIX)/bin

uninstall:
	@$(RM) $(DESTDIR)$(PREFIX)/bin/pinch
