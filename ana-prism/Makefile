.PHONY: all clean
.DEFAULT_GOAL := all

LIBS=-lrt -lm -lstdc++fs -lssl -lcrypto
INCLUDES=-./include
CFLAGS=-std=c++17 -O3 -g

output = ana_prism

all: main

main: p_analyze.cc
	g++ $(CFLAGS) -o ana_prism p_analyze.cc $(LIBS)

clean:
	rm $(output)
