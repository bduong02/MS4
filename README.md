# 4300-Butterfly
DB Relation Manager project for CPSC4300 at Seattle U, Spring 2023, Project Butterfly
Sprint Otono by David Stanko and Ryan Silveira

To run shell with tests, run make, and then ./sql4300 database_directory_path

Tests are located in ms4Tests.cpp (ran in Shell.cpp)
Milestones 3 and 4 added table and index options.  Primary modifications were made with the SQLExec file.
CREATE TABLE, DROP TABLE, SHOW TABLES, SHOW COLUMNS, CREATE INDEX, SHOW INDEX, and DROP INDEX now all have functionality.

USAGE - 
1) compile the program with make
2) run the program with ./sql4300 database_directory_path
3) Run with user input 
    -SQL Create Table and Select statements
    -tests run automatically on beginning of program
    -quit exits the program