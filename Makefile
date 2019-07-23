.PHONY: image install uninstall

DESTDIR ?=
PREFIX  ?= /usr/local

image:
	docker build -t pinch .

install: image
	@install -v -d -m 755 $(DESTDIR)$(PREFIX)/bin
	@install -v pinch $(DESTDIR)$(PREFIX)/bin

uninstall:
	@$(RM) $(DESTDIR)$(PREFIX)/bin/pinch
