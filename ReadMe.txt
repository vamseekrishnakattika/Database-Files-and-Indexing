						Database Design Programming Project 2
------------------------------------------------------------------------------------------------------------------------------------------
Download the project from eLearning.

Using Eclipse IDE:

Import the zip folder using File->Import->General->Projects from Folder orArchive.

Click on Archive and select the downloaded zip file.

In the project explorer clcik on SqLite.java and made it as the current selection file

Run the code using Run button.

The console will be opened and "SqLite>" prompt will be there. The program will run until you use the command "exit;".

To display supported commands, type "help;"

The supported commands are as follows.

	Command								Operation
---------------------------      		       	 	------------------------

SHOW TABLES;                                         		 Displays all the tables in the database.


CREATE TABLE table_name (<column_name datatype>);    		 Creates a new table in the database.


INSERT INTO table_name VALUES (value1,value2,..);     	 	 Inserts a new record into the table.

DELETE FROM TABLE table_name WHERE row_id = key_value;    	 Deletes a record from the table whose rowid is <key_value>.

UPDATE table_name SET column_name = value WHERE condition; 	 Modifies the records in the table.		

SELECT * FROM table_name;                                  	 Displays all records in the table.

SELECT * FROM table_name WHERE column_name operator value; 	 Displays records in the table where the given condition is satisfied


DROP TABLE table_name;                                     	 Removes table data and its schema.


VERSION;                                                   	 Shows the program version.		

HELP;                                                      	 Shows the help information.

EXIT;                                                      	 Exits the program.

Please note that indentation should be followed. Otherwise the commands will not executed properly.

While creating a table the first value should be int as it is the primary key.