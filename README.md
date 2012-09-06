FluentCassandra
===============

FluentCassandra is a .NET library for accessing Apache Cassandra.  It fully impliments all commands that can be issued against the Apache Cassandra interface and supports .NET 4.0 dynamic keyword as well as a LINQ like expressions for querying the database.  The goal of this project is to keep the interface in sync with the latest version of Cassandra and make it as easy as possible for .NET developers to start, adopt and program against the Cassandra database.

Getting Help
------------

You can get help with FluentCassandra by asking questions or starting a discussion on our group.  https://groups.google.com/d/forum/fluentcassandra

Installing Cassandra
--------------------

Taken from the "[Cassandra Jump Start For Windows](http://www.coderjournal.com/2010/03/cassandra-jump-start-for-the-windows-developer/)" by Nick Berardi: 

 1. Download Cassandra from http://cassandra.apache.org/
 2. Extract Cassandra to a directory of your choice (I used c:\development\cassandra)
 3. Set the following environment variables

        JAVA_HOME (To the directory where you install the JRE, this should not be the bin directory)
        CASSANDRA_HOME (To the directory you extracted the files to in step 1)

    *Please note that you are going to want to be running Java JRE 6 for running Cassandra.*

 4. Modify your Cassandra config file as you like and don't forget to update the directory locations from a UNIX like path to something on your windows directory (in my example the config file is located at c:\development\cassandra\conf\storage-conf.xml)
 5. Open cmd and run the cassandra.bat file (in my example the batch file is located at c:\development\cassandra\bin\cassandra.bat) 

        cd c:\development\cassandra\bin\
        .\cassandra.bat

 6. You can verify that Cassandra is running, by trying to connect to the server.  To do this open a new cmd and run the cassandra-cli.bat file from the bin directory.

        cd c:\development\cassandra\bin\
        .\cassandra-cli.bat
        connect localhost/9160

Your First Fluent Cassandra Application
--------------------

There was an indepth 2 part series of blog posts made on the subject of creating your first Fluent Cassandra application.

1. [Your First Fluent Cassandra Application](http://coderjournal.com/2010/06/your-first-fluent-cassandra-application/)
2. [Your First Fluent Cassandra Application (part 2)](http://coderjournal.com/2010/06/your-first-fluent-cassandra-application-part-2/)