package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/bradfitz/slice"
	"github.com/jessevdk/go-flags"
)

type Executor struct {
	dynamo   *dynamodb.DynamoDB
	tableCtx *TableContext
}

func newExecutor(dynamo *dynamodb.DynamoDB, tableCtx *TableContext) Executor {
	return Executor{dynamo: dynamo, tableCtx: tableCtx}
}

type readOpts struct {
	Projection       string `short:"p" long:"projection" description:"Projection expression" required:"false"`
	Filter           string `short:"f" long:"filter" description:"Filter expression" required:"false"`
	Index            string `short:"i" long:"index" description:"Index name" required:"false"`
	ConsistentRead   bool   `short:"c" long:"consistent-read" description:"Set consistent-read to true" required:"false"`
	ConsumedCapacity string `short:"r" long:"returned-consumed-capacity" description:"Return consumed capacity" required:"false"`
	Select           string `short:"s" long:"select" description:"Select" required:"false"`
	Limit            *int64 `short:"l" long:"limit" description:"Maximum items returned, equivalent to --max-items" required:"false"`
	//TODO
	//StartingToken    string `short:"t" long:"starting-token" description:"Starting token" required:"false"`
}

type queryOpts struct {
	readOpts
	Key                string `short:"k" long:"key" description:"Key expression" required:"true"`
	NoScanIndexForward bool   `short:"n" long:"no-scan-index-forward" description:"Set NoScanIndexForward to true" required:"false"`
}

type scanOpts struct {
	readOpts
	TotalSegments *int64 `long:"total-segments" description:"Total segments" required:"false"`
	Segment       *int64 `long:"segment" description:"Segment" required:"false"`
}

func (e Executor) Execute(input string) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println(r)
		}
	}()

	e.handleInput(input)
}

func (e Executor) handleInput(input string) {
	firstSeparatorIdx := strings.Index(input, " ")

	var command string = input
	if firstSeparatorIdx != -1 {
		command = input[:firstSeparatorIdx]
	}
	args := input[firstSeparatorIdx+1:]

	switch command {
	case "":
		return
	case "use":
		e.handleUse(args)
	case "quit":
		fallthrough
	case "q":
		fallthrough
	case "exit":
		fmt.Println("Goodbye")
		os.Exit(0)
	case "query":
		e.handleQuery(args)
	case "scan":
		e.handleScan(args)
	case "desc":
		e.handleDesc()
	default:
		fmt.Println("Unknown command: " + command)
	}
}

func (e Executor) handleUse(tableName string) {
	output, err := e.dynamo.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: &tableName,
	})

	if err != nil {
		if strings.Contains(err.Error(), "ResourceNotFoundException") {
			fmt.Printf("Couldn't find table with name: %s\n", tableName)
			return
		}

		panic(err)
	}

	e.tableCtx.name = *output.Table.TableName

	for _, s := range output.Table.KeySchema {
		if *s.KeyType == "HASH" {
			e.tableCtx.hashAttributeName = *s.AttributeName
		}
		if *s.KeyType == "RANGE" {
			e.tableCtx.rangeAttributeName = *s.AttributeName
		}
	}

	indexNames := []string{}
	for _, gsi := range output.Table.GlobalSecondaryIndexes {
		indexNames = append(indexNames, *gsi.IndexName)
	}
	for _, lsi := range output.Table.LocalSecondaryIndexes {
		indexNames = append(indexNames, *lsi.IndexName)
	}

	e.tableCtx.indexNames = indexNames
}

func (e Executor) handleQuery(args string) {
	e.validateTableSelected()

	queryOpts := queryOpts{}

	_, err := flags.ParseArgs(&queryOpts, parseArgs(args))
	if err != nil {
		return
	}

	expr := parseQuery(queryOpts.Key, queryOpts.Filter, queryOpts.Projection)

	queryInput := dynamodb.QueryInput{
		TableName:                 &e.tableCtx.name,
		ExpressionAttributeNames:  expr.getNames(),
		ExpressionAttributeValues: expr.getValues(),
		KeyConditionExpression:    expr.keyExpr(),
		FilterExpression:          expr.filterExpr(),
		ProjectionExpression:      expr.projExpr(),
	}

	if queryOpts.Index != "" {
		queryInput.SetIndexName(queryOpts.Index)
	}
	if queryOpts.ConsistentRead {
		queryInput.SetConsistentRead(true)
	}
	if queryOpts.ConsumedCapacity != "" {
		queryInput.SetReturnConsumedCapacity(queryOpts.ConsumedCapacity)
	}
	if queryOpts.Select != "" {
		queryInput.SetSelect(queryOpts.Select)
	}
	if queryOpts.Limit != nil {
		queryInput.SetLimit(*queryOpts.Limit)
	}
	if queryOpts.NoScanIndexForward {
		queryInput.SetScanIndexForward(false)
	}

	if opts.Verbose {
		fmt.Printf("DEBUG query input: %v\n", queryInput)
	}

	queryOutput, err := e.dynamo.Query(&queryInput)
	if err != nil {
		if !opts.Verbose {
			// If debug is off, print whole input anyway
			fmt.Printf("DEBUG query input: %v\n", queryInput)
		}
		fmt.Println("Error occurred: ", err)
	} else {
		fmt.Println(prettify(queryOutput))
	}
}

func (e Executor) handleScan(args string) {
	e.validateTableSelected()

	scanOpts := scanOpts{}

	_, err := flags.ParseArgs(&scanOpts, parseArgs(args))
	if err != nil {
		return
	}

	expr := parseQuery("", scanOpts.Filter, scanOpts.Projection)

	scanInput := dynamodb.ScanInput{
		TableName:                 &e.tableCtx.name,
		ExpressionAttributeNames:  expr.getNames(),
		ExpressionAttributeValues: expr.getValues(),
		FilterExpression:          expr.filterExpr(),
		ProjectionExpression:      expr.projExpr(),
	}

	if scanOpts.Index != "" {
		scanInput.SetIndexName(scanOpts.Index)
	}
	if scanOpts.ConsistentRead {
		scanInput.SetConsistentRead(true)
	}
	if scanOpts.ConsumedCapacity != "" {
		scanInput.SetReturnConsumedCapacity(scanOpts.ConsumedCapacity)
	}
	if scanOpts.Select != "" {
		scanInput.SetSelect(scanOpts.Select)
	}
	if scanOpts.Limit != nil {
		scanInput.SetLimit(*scanOpts.Limit)
	}
	if scanOpts.Segment != nil {
		scanInput.SetSegment(*scanOpts.Segment)
	}
	if scanOpts.TotalSegments != nil {
		scanInput.SetTotalSegments(*scanOpts.TotalSegments)
	}

	if opts.Verbose {
		fmt.Printf("DEBUG scan input: %v\n", scanInput)
	}

	scanOutput, err := e.dynamo.Scan(&scanInput)
	if err != nil {
		fmt.Println("Error occurred: ", err)
	} else {
		fmt.Println(prettify(scanOutput))
	}
}

func (e Executor) handleDesc() {
	e.validateTableSelected()

	describeInput := dynamodb.DescribeTableInput{
		TableName: &e.tableCtx.name,
	}

	describeOutput, err := e.dynamo.DescribeTable(&describeInput)
	if err != nil {
		fmt.Println("Error occurred: ", err)
	} else {
		fmt.Println(describeOutput)
	}
}

func (e Executor) validateTableSelected() {
	if e.tableCtx.name == "" {
		panic("No table selected!")
	}

}

// split args by ' ' and group quoted args
func parseArgs(args string) []string {
	var parsedArgs []string
	var start int = 0
	var startedString int = -1

	for pos, char := range args {
		// An unescape double quote always either starts or terminates the double quoted argument.
		// If a double quote has multiple escape symbols ("\") before it, it must be in a string value, so we can ignore that case
		if char == '"' && args[pos-1] != '\\' {
			if startedString == -1 {
				startedString = pos + 1
			} else {
				parsedArgs = append(parsedArgs, args[startedString:pos])
				startedString = -1
			}
		}

		if startedString == -1 && (pos == len(args)-1 || args[pos+1] == ' ') {
			parsedArgs = append(parsedArgs, args[start:pos+1])
			start = pos + 2
		}
	}

	return parsedArgs
}

// This is just awsutil.Prettify with a few modifications - displaying map entries in alphabetical order and
// squishing simple value output to a single line.
// Prettify returns the string representation of a value.
func prettify(i interface{}) string {
	var buf bytes.Buffer
	prettify0(reflect.ValueOf(i), 0, &buf)
	return buf.String()
}

// prettify will recursively walk value v to build a textual
// representation of the value.
func prettify0(v reflect.Value, indent int, buf *bytes.Buffer) {
	for v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	switch v.Kind() {
	case reflect.Struct:
		strtype := v.Type().String()
		if strtype == "time.Time" {
			fmt.Fprintf(buf, "%s", v.Interface())
			break
		} else if strings.HasPrefix(strtype, "io.") {
			buf.WriteString("<buffer>")
			break
		}

		isAttributeValue := strtype == "dynamodb.AttributeValue"

		buf.WriteString("{")
		if isAttributeValue {
			buf.WriteString(" ")
		} else {
			buf.WriteString("\n")
		}

		names := []string{}
		for i := 0; i < v.Type().NumField(); i++ {
			name := v.Type().Field(i).Name
			f := v.Field(i)
			if name[0:1] == strings.ToLower(name[0:1]) {
				continue // ignore unexported fields
			}
			if (f.Kind() == reflect.Ptr || f.Kind() == reflect.Slice || f.Kind() == reflect.Map) && f.IsNil() {
				continue // ignore unset fields
			}
			names = append(names, name)
		}

		for i, n := range names {
			val := v.FieldByName(n)
			if !isAttributeValue {
				buf.WriteString(strings.Repeat(" ", indent+2))
			}
			buf.WriteString(n + ": ")
			prettify0(val, indent+2, buf)

			if i < len(names)-1 {
				buf.WriteString(",\n")
			}
		}

		if isAttributeValue {
			buf.WriteString(" }")
		} else {
			buf.WriteString("\n" + strings.Repeat(" ", indent) + "}")
		}
	case reflect.Slice:
		strtype := v.Type().String()
		if strtype == "[]uint8" {
			fmt.Fprintf(buf, "<binary> len %d", v.Len())
			break
		}

		nl, id, id2 := "", "", ""
		if v.Len() > 3 {
			nl, id, id2 = "\n", strings.Repeat(" ", indent), strings.Repeat(" ", indent+2)
		}
		buf.WriteString("[" + nl)
		for i := 0; i < v.Len(); i++ {
			buf.WriteString(id2)
			prettify0(v.Index(i), indent+2, buf)

			if i < v.Len()-1 {
				buf.WriteString("," + nl)
			}
		}

		buf.WriteString(nl + id + "]")
	case reflect.Map:
		buf.WriteString("{\n")

		keys := v.MapKeys()

		slice.Sort(keys[:], func(i, j int) bool {
			return keys[i].String() < keys[j].String()
		})

		for i, k := range keys {
			buf.WriteString(strings.Repeat(" ", indent+2))
			buf.WriteString(k.String() + ": ")
			prettify0(v.MapIndex(k), indent+2, buf)

			if i < v.Len()-1 {
				buf.WriteString(",\n")
			}
		}

		buf.WriteString("\n" + strings.Repeat(" ", indent) + "}")
	default:
		if !v.IsValid() {
			fmt.Fprint(buf, "<invalid value>")
			return
		}
		format := "%v"
		switch v.Interface().(type) {
		case string:
			format = "%q"
		case io.ReadSeeker, io.Reader:
			format = "buffer(%p)"
		}
		fmt.Fprintf(buf, format, v.Interface())
	}
}
