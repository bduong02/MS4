# 4300-Butterfly
## Description
DB Relation Manager project for CPSC4300 at Seattle U, Spring 2023, Project Butterfly

### Milestone 1
Take SQL statements, parse them, and print out the parse tree.

### Milestone 2
Implements Heap Storage Engine's version of each: SlottedPage, HeapFile, and HeapTable.

### Milestone 3
Executes `CREATE`, `DROP`, and `SHOW` SQL statements in a Berkeley DB Database. `SHOW` statements can handle displaying tables and columns.

### Milestone 4
Builds off of Milestone 3 to `CREATE`, `DROP`, and `SHOW` indices as well as tables.

### Milestone 5
Implement basic `INSERT` and `SELECT` statements, and the skeleton for BEGIN / COMMIT / ROLLBACK.

### Milestone 6
Creates locks to handle transactions running in parallel.

## Installation
1. Clone the repository on CS1

` git clone https://github.com/BguardiaOpen/4300-Butterfly23SQ.git `

2. Ensure the ` .bash_profile ` path is configured correctly
```
$ cd # cd without an argument says to go to home directory, ~
$ vim .bash_profile 
```
```
export PATH=/usr/local/db6/bin:$PATH
export LD_LIBRARY_PATH=/usr/local/db6/lib:$LD_LIBRARY_PATH
export PYTHONPATH=/usr/local/db6/lib/site-packages:$PYTHONPATH 
```

## Usage
1. Create a directory to hold the database (first time usage only)
2. Compile the program with ` make `
3. Run the program with ` ./cpsc4300 path_to_database_directory `
    
    * The path must be the path to the directory from the root user@cs1
4. Other ``` make ``` options
    
    * ` make clean `: removes the object code files
5. User input options
    * SQL `CREATE`, `DROP`, and `SHOW` statements (see example Milestone 3 and 4)
    * SQL `INSERT` and `SELECT` statements (see example Milestone 5 and 6
    #Milestone 3 and 4
    * ` quit ` exits the program

## Example Milestone 3 and 4

```
$ ./cpsc4300 cpsc4300/data
SQL> create table foo (a text, b integer)
CREATE TABLE foo (a TEXT, b INT)
SQL> show tables
SHOW TABLES
table_name 
+----------+
"foo" 
SQL> show columns from foo
SHOW COLUMNS FROM foo
table_name column_name data_type 
+----------+----------+----------+
"foo" "a" "TEXT" 
"foo" "b" "INT"
SQL> drop table foo
DROP TABLE foo
SQL> show tables
SHOW TABLES
table_name 
+----------+
SQL> create index fx on foo (a)
CREATE INDEX fx ON foo USING BTREE (a)
created index fx
SQL> show index from foo
SHOW INDEX FROM goober
table_name index_name column_name seq_in_index index_type is_unique 
+----------+----------+----------+----------+----------+----------+
"foo" "fx" "a" 1 "BTREE" true
SQL> not real sql
Invalid SQL: not real sql
SQL> quit
```
## Example Milestone 5 and 6
### Instance 1
```
SQL> create table foo (id int, data text)
CREATE TABLE foo (id INT, data TEXT)
created foo
SQL> insert into foo values (1,"one");insert into foo values(2,"two"); insert into foo values (2, "another two")
INSERT INTO foo VALUES (1, "one")
successfully inserted 1 row into foo
INSERT INTO foo VALUES (2, "two")
successfully inserted 1 row into foo
INSERT INTO foo VALUES (2, "another two")
successfully inserted 1 row into foo
SQL> select * from foo
SELECT * FROM foo
id data 
+----------+----------+
1 "one" 
2 "two" 
2 "another two" 
successfully returned 3 rows
SQL> begin transaction
BEGIN TRANSACTION - level 1
SQL> insert into foo values (4,"four");
INSERT INTO foo VALUES (4, "four")
successfully inserted 1 row into foo
SQL> select * from foo
SELECT * FROM foo
id data 
+----------+----------+
1 "one" 
2 "two" 
2 "another two" 
4 "four" 
successfully returned 4 rows
SQL> commit transaction
successfully committed transaction level 1
SQL> quit
```
### Instance 2, right after Instance 1 ran begin transaction:
```
SQL> select * from foo
SELECT * FROM foo
id data 
+----------+----------+
1 "one" 
2 "two" 
2 "another two" 
successfully returned 3 rows
```
### Instance 2, right after Instance 1 ran INSERT INTO foo VALUES (4, "four"):
```
SQL> select * from foo
SELECT * FROM foo
Waiting for lock - table foo

id data 
+----------+----------+
1 "one" 
2 "two" 
2 "another two" 
4 "four" 
successfully returned 4 rows
```

## Acknowledgements
* [Berkeley DB](https://www.oracle.com/database/technologies/related/berkeleydb.html)
* [Berkeley DB Dbt](https://docs.oracle.com/cd/E17076_05/html/api_reference/CXX/frame_main.html)
* [Hyrise SQL Parser](https://github.com/klundeen/sql-parser)
* [Professor Lundeen's 5300-Instructor base code](https://github.com/klundeen/5300-Instructor/releases/tag/Milestone2h) 🙏🙏
* [The Python equivalent](https://github.com/BguardiaOpen/cpsc4300py)
