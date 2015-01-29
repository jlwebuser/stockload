//
// Creates keyspace for stock history data
// Run using cqlsh <node> -f <thisfile>
//

create keyspace if not exists stocks with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

create table if not exists stocks.history(	symbol ascii, 
				yyyymmdd ascii,
				open float,
				high float, 
				low float, 
				close float,
				volume float,
				PRIMARY KEY(symbol, yyyymmdd));



