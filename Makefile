.PHONY: all clean
.DEFAULT_GOAL := all

LIBS=-lrt -lm -lstdc++fs -lssl -lcrypto -llustreapi -lpthread
INCLUDES=-./include
CFLAGS=-std=c++17 -O0 -g -fpermissive

MPICXX=mpicxx

output = danzer_obj

all: main

main: mpitracer.cc
	$(MPICXX) $(CFLAGS) -o danzer_obj mpitracer.cc master.cc $(LIBS)

clean:
	rm $(output)
