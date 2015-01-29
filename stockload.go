package main

import (
	"encoding/csv"
	"fmt"
	"github.com/gocql/gocql"
	"log"
	"os"
	"strconv"
	"strings"
)

//
//  stockload <filename>
//
//  read the file name specificed in the last arg
//

func main() {
	// Get the filename off the command line
	if len(os.Args) != 2 {
		fmt.Printf("usage: %s <file_SYMBOL.csv>\n", os.Args[0])
		os.Exit(1)
	}

	// Verify the symbol is encoded in the file name correctly
	symbol := getSymbol(os.Args[1])
	if symbol == "" {
		fmt.Printf("file name must encode stock symbol base_XXXX.csv\n")
		os.Exit(1)
	}

	// Open the file and initialize the csv reader
	csvfile, err := os.Open(os.Args[1])
	if err != nil {
		fmt.Println(err)
		return
	}
	defer csvfile.Close()
	reader := csv.NewReader(csvfile)
	rawCSVdata, err := reader.ReadAll()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// Expecting 7 columns in the quote format
	if reader.FieldsPerRecord != 7 {
		fmt.Printf("Expected 7, but got %d columns in first row\n", reader.FieldsPerRecord)
		os.Exit(1)
	}

	// Status output
	fmt.Printf("%s: Starting to load %d rows by %d columns of data for symbol %s\n",
		os.Args[0], len(rawCSVdata), len(rawCSVdata[0]), symbol)

	dumpSample(rawCSVdata, 10)

	// load the database and write out status lines
	fmt.Printf("\n%s: loaded %d rows\n", os.Args[0], loadRecords(symbol, rawCSVdata))
}

//
// Upload the vector of data for the specified stock symbol
//
func loadRecords(symbol string, records [][]string) int {
	cluster := gocql.NewCluster("127.0.0.1")
	cluster.Keyspace = "stocks"
	cluster.Consistency = gocql.Quorum
	session, _ := cluster.CreateSession()
	defer session.Close()
	count := 0
	var stub string
	var qexec string
	stub = "INSERT INTO history (symbol, yyyymmdd, open, high, low, close, volume) VALUES "

	// Load the records
	for _, each := range records {
		yyyymmdd := each[0]
		open, _ := strconv.ParseFloat(each[2], 32)
		high, _ := strconv.ParseFloat(each[3], 32)
		low, _ := strconv.ParseFloat(each[4], 32)
		close, _ := strconv.ParseFloat(each[5], 32)
		volume, _ := strconv.ParseFloat(each[6], 32)

		qexec = fmt.Sprintf("%s ('%s', '%s', %f, %f, %f, %f, %f)", stub, symbol, yyyymmdd, open, high, low, close, volume)

		err := session.Query(qexec).Exec()
		if err != nil {
			log.Fatal(err)
		}
		count++
		if count%100 == 0 {
			fmt.Printf("%d %s\n", count, qexec)
		}
	}
	return count
}

//
// Extract the stock symbol name encoded in the file name.
// The format must be ...._XXXX.csv
//
func getSymbol(s string) string {
	t := strings.TrimSuffix(s, ".csv")
	if s == t {
		return ""
	}
	index := strings.LastIndex(t, "_")
	if index <= 0 {
		return ""
	}
	return strings.ToUpper(t[index+1:])
}

//
// Debugging function to dump out a sample of the data
//
func dumpSample(records [][]string, n int) {
	// sanity check, display to standard output
	count := 0
	for _, each := range records {
		fmt.Printf("%s %s %s %s %s %s %s\n",
			each[0], each[1], each[2], each[3], each[4], each[5], each[6])
		count += 1
		if count > n {
			break
		}
	}
	fmt.Printf("\n")
}
