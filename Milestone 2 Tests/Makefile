CPP=g++
CFLAGS=-c -Wall -Werror -std=c++17

all: tests

tests: SlottedPageTests

SlottedPageTests: SlottedPageTests.cpp 
	g++ SlottedPageTests.cpp -lgtest -lpthread -o SlottedPageTests


clean: 
	rm -f SlottedPageTests