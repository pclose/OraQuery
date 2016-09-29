#OraQuery
-----

Program to export data in an Oracle database to CSV

##Build and Run  

1. Download ojdbc6.jar from [Oracle](http://www.oracle.com/technetwork/apps-tech/jdbc-112010-090769.html)  

2. Download opencsv-x.x.jar from [Sourceforge](http://opencsv.sourceforge.net/)

3. Compile `javac -cp "./*" src/OraQuery.java`  

4. Run `java -cp "./*:./src/" OraQuery <db_host> <db_user> <db_password> [-i <sql_file>] [-o <output_file>]`  

Alternatively you could compile using ant.  
This will also bundle an executable jar under ./build/bin/  
`ant -buildfile ant/build.xml`
