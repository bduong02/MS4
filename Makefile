Parser: Parser.o
	g++ -L/usr/local/db6/lib -L/usr/local/db6/lib  -o $@ $< -ldb_cxx -lsqlparser

Parser.o : Parser.cpp
	g++ -I/usr/local/db6/include -I/usr/local/db6/include -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT -O3 -std=c++11 -c -o $@ $<

clean : 
	rm -f Parser.o Parser