When on Micrsoft SQL Server Management Studio, click the Connect button and select "Database Engine". 
First, connect to SQL Server and restore the databases from the backup (.bak) files.

After that, you want to go in and make sure all of the needed infromation is in the correct database (in this case the database named "source").

Once you verify there is infromation in the source database, you want to go into the second database (for this project named "destination") and purge it of any data that is unneccesary. 

To begin preparation for data migration, you want to go in and establish a connection to the source database (of course after importing all necessary libraries).
  *A connection to the destination database is also needed

After establishing the connections, you go on to implement the python code to migrate the information you want. 
