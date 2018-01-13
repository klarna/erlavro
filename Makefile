REBAR ?= rebar3

all: compile

.PHONY: elvis-rock
elvis-rock:
	elvis rock

.PHONY: compile
compile: deps
	$(REBAR) compile

.PHONY: deps
deps:
	$(REBAR) get-deps

.PHONY: edoc
edoc:
	$(REBAR) edoc

.PHONY: dialyzer
dialyzer: compile
	$(REBAR) dialyzer

.PHONY: eunit
eunit:
	$(REBAR) eunit -v

.PHONY: clean
clean:
	$(REBAR) clean

.PHONY: xref
xref: compile
	$(REBAR) xref

.PHONY: hex-publish
hex-publish: clean
	$(REBAR) hex publish

.PHONY: cover
cover:
	$(REBAR) cover -v

