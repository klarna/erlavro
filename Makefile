REBAR ?= $(shell which rebar || which ./rebar 2>/dev/null)

suite=$(if $(SUITE), suite=$(SUITE), )

.PHONY:	all deps check test clean

all: compile

compile: deps
	$(REBAR) compile

deps:
	$(REBAR) get-deps

docs:
	$(REBAR) doc

check:
	$(REBAR) check-plt
	$(REBAR) dialyze

test: compile
	$(REBAR) eunit $(suite) apps=erlavro

conf_clean:
	@:

clean:
	$(REBAR) clean
	$(RM) doc/*

xref: compile
	$(REBAR) xref


# eof
