# Makefile. 
# Authors: Ishan Parikh, Kevin, 
# CPSC4300, Spring 2023
# Team: Butterfly

EXECUTABLE_FILE = sql4300
LIB = /usr/local/db6/lib
OBJS = sql5300.o ParseTreeToString.o SQLExec.o schema_tables.o storage_engine.o

sql4300: $(OBJS)
	g++ -L$(LIB) -o $@ $(OBJS) -ldb_cxx -lsqlparser

HEAP_STORAGE_H = heap_storage.h storage_engine.h
SCHEMA_TABLES_H = schema_tables.h $(HEAP_STORAGE_H)
SQLEXEC_H = SQLExec.h $(SCHEMA_TABLES_H)
ParseTreeToString.o : ParseTreeToString.h
SQLExec.o : $(SQLEXEC_H)
schema_tables.o : $(SCHEMA_TABLES_) ParseTreeToString.h
sql5300.o : $(SQLEXEC_H) ParseTreeToString.h
storage_engine.o : storage_engine.h

#compilation rules for all object files
%.o : %.cpp
	g++ -I/usr/local/db6/include -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT -fpermissive -O3 -std=c++11 -c -o $@ $<

clean : 
	rm -f Shell sql4300 *.o
