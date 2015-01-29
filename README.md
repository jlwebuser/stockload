# stockload
This is a work in progress of using go to load stock quotes in CSV format into cassandra using gocql.

1 after installing cassandra, use cqlsh to run the init file that creates the keyspace for the stock history table.
2 you can download historical stock quote files and format information from from https://quantquote.com/historical-stock-data
3 just unzip the file and load the stocks you want. 

I will expand the model and improve the loader in the future. 
My goal is build a basic loader and model that dumps stock closing data into cassandra, and then build a set of analytical tools on top.
