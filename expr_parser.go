package main

import (
	"errors"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

// Don't include "<<", "[" in specials, as we want them left intact for set/list parsing
var specials []string = []string{" ", ",", "(", ")", "]", ">>"}
var comparators []string = []string{"=", ">", "<", ">=", "<=", "<>"}
var keywords []string = append(append(specials, comparators...),
	"and", "or", "not", "in", "between",
	"set", "remove", "add", "delete", "+", "-",
	"begins_with", "attribute_exists", "attribute_not_exists", "attribute_type", "contains", "size", "list_append", "if_not_exists",
)

type params interface {
	keyExpr() *string
	filterExpr() *string
	projExpr() *string
	getNames() map[string]*string
	getValues() map[string]*dynamodb.AttributeValue
}

type paramsImpl struct {
	nameIdx, valueIdx *int
	key, filter, proj *string
	names             map[string]*string
	values            map[string]*dynamodb.AttributeValue
}

func (q paramsImpl) keyExpr() *string {
	if q.key != nil && "" == strings.Trim(*q.key, " ") {
		return nil
	}

	return q.key
}

func (q paramsImpl) filterExpr() *string {
	if q.filter != nil && "" == strings.Trim(*q.filter, " ") {
		return nil
	}

	return q.filter
}

func (q paramsImpl) projExpr() *string {
	if q.proj != nil && "" == strings.Trim(*q.proj, " ") {
		return nil
	}

	return q.proj
}

func (q paramsImpl) getNames() map[string]*string {
	if q.names == nil || len(q.names) == 0 {
		return nil
	}

	return q.names
}

func (q paramsImpl) getValues() map[string]*dynamodb.AttributeValue {
	if q.values == nil || len(q.values) == 0 {
		return nil
	}

	return q.values
}

func (q paramsImpl) addName(name string) string {
	placeholder := "#" + strconv.Itoa(*q.nameIdx)
	q.names[placeholder] = &name
	*q.nameIdx++

	return placeholder
}

func (q paramsImpl) addValue(value *dynamodb.AttributeValue) string {
	placeholder := ":" + strconv.Itoa(*q.valueIdx)
	q.values[placeholder] = value
	*q.valueIdx++

	return placeholder
}

func parseQuery(keyInput string, filterInput string, projInput string) params {
	namesIdx := 0
	valueIdx := 0

	params := paramsImpl{
		nameIdx:  &namesIdx,
		valueIdx: &valueIdx,
		key:      nil,
		filter:   nil,
		proj:     nil,
		names:    make(map[string]*string),
		values:   make(map[string]*dynamodb.AttributeValue),
	}

	params.key = parseGenericExpression(keyInput, params)
	params.filter = parseGenericExpression(filterInput, params)
	params.proj = parseProjectionExpression(projInput, params)

	return params
}

// parses key and filter expressions
// updates params' names and values
// TODO maybe this can parse update and condition expressions as well?
func parseGenericExpression(expr string, params paramsImpl) (resultExpr *string) {
	resultExpr = new(string)

	for len(expr) > 0 {
		// Try to parse keyword
		keyword := startsWithKeyword(expr)

		if keyword != nil {
			*resultExpr = *resultExpr + *keyword
			expr = expr[len(*keyword):]
			continue
		}

		// Try to parse value
		value, remainder, err := parseValue(expr)
		if err == nil {
			withValue := *resultExpr + params.addValue(value)
			*resultExpr = withValue
			expr = remainder
			continue
		}

		resultName, remainder := parseName(expr, params)
		withName := *resultExpr + resultName
		*resultExpr = withName
		expr = remainder
	}

	return resultExpr
}

// When names are nested, they need to be placeholdered separately
// When they're indexed, the index should be included raw (i.e. not in the placeholder value)
// e.g. a.b[1].c needs to become #1.#2[1].#3
func parseName(expr string, params paramsImpl) (resultName string, remainder string) {
	isEscaped := false
	if strings.HasPrefix(expr, "`") {
		isEscaped = true
		expr = expr[1:]
	}

	var parsedName string
	if isEscaped { // if escape just read until closing tilde
		indexEnd := strings.Index(expr, "`")
		parsedName = expr[:indexEnd]
		remainder = expr[indexEnd+1:]
	} else {
		parsedName, remainder = parseNextToken(expr, ".", "[")
	}

	resultName = params.addName(parsedName)

	if len(remainder) > 0 && remainder[0] == '[' {
		closingIdx := findWithOffset(remainder, "]", 1)
		resultName = resultName + remainder[:closingIdx+1]
		remainder = remainder[closingIdx+1:]
	}

	if len(remainder) > 0 && remainder[0] == '.' {
		parsedName, remainder = parseName(remainder[1:], params)
		resultName = resultName + "." + parsedName
	}

	return resultName, remainder
}

// returns the next token, as terminated by either a special or the end of the string
// the returned remainder starts with the termination character
func parseNextToken(expr string, additionalDelimiters ...string) (attributeName string, remainder string) {
	delimiterIndexes := []int{len(expr)}

	for _, s := range append(append(append(specials, comparators...)), additionalDelimiters...) {
		idx := strings.Index(expr, string(s))
		if idx != -1 {
			delimiterIndexes = append(delimiterIndexes, idx)
		}
	}

	smallest := delimiterIndexes[0]

	for _, num := range delimiterIndexes {
		if num < smallest {
			smallest = num
		}
	}

	attributeName = expr[:smallest]
	expr = expr[smallest:]

	return attributeName, expr
}

func parseValue(expr string) (*dynamodb.AttributeValue, string, error) {
	var value interface{}

	value, remainder, err := tryParseString(expr)
	if err != nil {
		value, remainder, err = tryParseNumber(expr)
	}
	if err != nil {
		value, remainder, err = tryParseBoolean(expr)
	}
	if err != nil {
		remainder, err = tryParseNull(expr)
		value = nil
	}
	if err != nil {
		value, remainder, err = tryParseList(expr)
	}
	if err != nil {
		value, remainder, err = tryParseMap(expr)
	}
	if err != nil {
		return nil, expr, errors.New("Could not parse value at: " + expr)
	}

	// lists and sets are a special case that return a *dynamodb.AttributeValue
	// that is already marshalled
	attributeValue, isAttributeValue := value.(*dynamodb.AttributeValue)
	if isAttributeValue {
		return attributeValue, remainder, nil
	}

	attributeValue, err = dynamodbattribute.Marshal(value)
	if err != nil {
		panic(err)
	}

	return attributeValue, remainder, nil
}

func tryParseString(expr string) (parsedStr string, remainder string, err error) {
	if expr[0] != '\'' {
		return "", expr, errors.New("Expected string value at: " + expr)
	}

	expr = expr[1:]

	idxQuote := strings.Index(expr, "'")
	if idxQuote == 0 {
		panic("Unterminated string")
	}

	idxEscape := strings.Index(expr, "\\")

	for idxEscape > -1 && idxEscape < idxQuote {
		if expr[idxEscape+1] == '"' || expr[idxEscape+1] == '\'' || expr[idxEscape+1] == '\\' {
			expr = expr[:idxEscape] + expr[idxEscape+1:]
			idxQuote = findWithOffset(expr, "'", idxEscape+1)
			idxEscape = findWithOffset(expr, "\\", idxEscape+1)
		} else {
			panic("Unexpected escape character")
		}
	}

	str := expr[:idxQuote]

	return str, expr[idxQuote+1:], nil
}

func findWithOffset(str string, search string, offset int) int {
	idx := strings.Index(str[offset:], search)

	if idx == -1 {
		return -1
	} else {
		return offset + idx
	}
}

func tryParseNumber(expr string) (parsedNum interface{}, remainder string, err error) {
	parsedNum, remainder, err = tryParseInt(expr)
	if err == nil {
		return parsedNum, remainder, nil
	}

	parsedNum, remainder, err = tryParseFloat(expr)
	if err == nil {
		return parsedNum, remainder, nil
	}

	return nil, expr, errors.New("Could not parse number")
}

func tryParseInt(expr string) (parsedInt int, remainder string, err error) {
	num, expr := parseNextToken(expr)
	parsedInt, parseErr := strconv.Atoi(num)

	return parsedInt, expr, parseErr
}

func tryParseFloat(expr string) (parsedNum float64, remainder string, err error) {
	num, expr := parseNextToken(expr)
	parsedFloat, parseErr := strconv.ParseFloat(num, 64)

	return parsedFloat, expr, parseErr
}

func tryParseBoolean(expr string) (result bool, remainder string, err error) {
	nextToken, expr := parseNextToken(expr)
	if strings.EqualFold(nextToken, "true") {
		return true, expr, nil
	}
	if strings.EqualFold(nextToken, "false") {
		return false, expr, nil
	}

	return false, expr, errors.New("Could not parse bool")
}

func tryParseNull(expr string) (remainder string, err error) {
	nextToken, expr := parseNextToken(expr)
	if strings.EqualFold(nextToken, "null") {
		return expr, nil
	}

	return expr, errors.New("Could not parse null")
}

func tryParseList(expr string) (list *dynamodb.AttributeValue, remainder string, err error) {
	if strings.HasPrefix(expr, "[") {
		expr = expr[1:]
	} else if strings.HasPrefix(expr, "<<") {
		expr = expr[2:]
	} else {
		return nil, expr, errors.New("Could not parse list")
	}

	expr = strings.TrimLeft(expr, " ,")

	items := []*dynamodb.AttributeValue{}
	for !strings.HasPrefix(expr, "]") && !strings.HasPrefix(expr, ">>") {
		val, remainder, err := parseValue(expr)

		if err != nil {
			panic(err)
		}

		items = append(items, val)
		expr = strings.TrimLeft(remainder, " ,")
	}

	if strings.HasPrefix(expr, "]") {
		return &dynamodb.AttributeValue{L: items}, expr[1:], nil
	}

	// Handle SS and NS
	set := &dynamodb.AttributeValue{}

	if items[0].N != nil {
		for _, n := range items {
			set.NS = append(set.NS, n.N)
		}
	} else {
		for _, s := range items {
			set.SS = append(set.SS, s.S)
		}
	}

	return set, expr[2:], nil

}

func tryParseMap(expr string) (result *dynamodb.AttributeValue, remainder string, err error) {
	if strings.HasPrefix(expr, "{") {
		expr = expr[1:]
	} else {
		return nil, expr, errors.New("Could not parse map")
	}

	expr = strings.TrimLeft(expr, " ")

	root := make(map[string]*dynamodb.AttributeValue)
	for !strings.HasPrefix(expr, "}") {
		colonIdx := findWithOffset(expr, ":", 0)

		name := strings.Trim(expr[0:colonIdx], " ")
		expr = strings.TrimLeft(expr[colonIdx+1:], " ")

		val, remainder, err := parseValue(expr)
		if err != nil {
			panic(err)
		}

		expr = strings.TrimLeft(remainder, " ,")
		root[name] = val
	}

	attributeValue := dynamodb.AttributeValue{M: root}
	return &attributeValue, expr[1:], nil
}

func parseProjectionExpression(expr string, params paramsImpl) *string {
	expr = strings.Trim(expr, " ")

	placeholderExpr := ""

	for len(expr) > 0 {
		name, remainder := parseName(expr, params)
		expr = remainder
		placeholderExpr = placeholderExpr + name

		if len(expr) > 0 {
			expr = strings.TrimLeft(expr[1:], " ")
			placeholderExpr = placeholderExpr + ","
		}
	}

	return &placeholderExpr
}

func startsWithKeyword(str string) *string {
	for _, prefix := range keywords {
		if strings.HasPrefix(strings.ToLower(str), prefix) {
			if prefix == "<" && strings.HasPrefix(str, "<<") {
				// edge case for set start - "<" is both a comparison operator and part of "<<"
				continue
			}
			found := str[:len(prefix)] // preserve case of input string
			return &found
		}
	}

	return nil
}
