.PHONY: image pipetee install uninstall acceptance test

DESTDIR ?=
PREFIX  ?= /usr/local

image:
	docker build -t jolynch/pinch .
	docker build -t jolynch/pinch-server server

acceptance: image test

test:
	tests/test_tools.sh
	tests/test_server.sh

pipetee:
	gcc pipetee/pipetee.c -o pipetee/pipetee

install: image pipetee
	@install -v -d -m 755 $(DESTDIR)$(PREFIX)/bin
	@install -v pinch $(DESTDIR)$(PREFIX)/bin
	@install -v pinch-server $(DESTDIR)$(PREFIX)/bin
	@install -v pipetee/pipetee $(DESTDIR)$(PREFIX)/bin

uninstall:
	@$(RM) $(DESTDIR)$(PREFIX)/bin/pinch
	@$(RM) $(DESTDIR)$(PREFIX)/bin/pinch-server
	@$(RM) $(DESTDIR)$(PREFIX)/bin/pipetee
