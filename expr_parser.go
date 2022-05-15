package main

import (
	"errors"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

// Don't include "<<", "[" in specials, as we want them left intact for set/list parsing
var specials []string = []string{" ", ",", "(", ")", "]", ">>", "=", ">", "<", ">=", "<=", "<>"}
var keywords []string = append(specials,
	"and", "or", "not", "in", "between",
	"set", "remove", "add", "delete", "+", "-",
	"begins_with", "attribute_exists", "attribute_not_exists", "attribute_type", "contains", "size", "list_append", "if_not_exists",
)

type exprParser interface {
	parseGenericExpression(expr string) *string
	parseProjectionExpression(expr string) *string
	getNames() map[string]*string
	getValues() map[string]*dynamodb.AttributeValue
}

type exprParserImpl struct {
	nameIdx, valueIdx *int
	key, filter, proj *string
	names             map[string]*string
	values            map[string]*dynamodb.AttributeValue
}

func newExprParser() exprParser {
	namesIdx, valueIdx := 0, 0

	return exprParserImpl{
		nameIdx:  &namesIdx,
		valueIdx: &valueIdx,
		names:    make(map[string]*string),
		values:   make(map[string]*dynamodb.AttributeValue),
	}
}

func (p exprParserImpl) getNames() map[string]*string {
	if p.names == nil || len(p.names) == 0 {
		return nil
	}

	return p.names
}

func (p exprParserImpl) getValues() map[string]*dynamodb.AttributeValue {
	if p.values == nil || len(p.values) == 0 {
		return nil
	}

	return p.values
}

func (p exprParserImpl) addName(name string) string {
	placeholder := "#" + strconv.Itoa(*p.nameIdx)
	p.names[placeholder] = &name
	*p.nameIdx++

	return placeholder
}

func (p exprParserImpl) addValue(value *dynamodb.AttributeValue) string {
	placeholder := ":" + strconv.Itoa(*p.valueIdx)
	p.values[placeholder] = value
	*p.valueIdx++

	return placeholder
}

// parses key, update, filter/condition expressions
// updates parser's names and values
func (p exprParserImpl) parseGenericExpression(expr string) (resultExpr *string) {
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
		value, remainder, _ := tryParseValue(expr)
		if value != nil {
			withValue := *resultExpr + p.addValue(value)
			*resultExpr = withValue
			expr = remainder
			continue
		}

		// Default to name
		resultName, remainder := p.parseName(expr)
		withName := *resultExpr + resultName
		*resultExpr = withName
		expr = remainder
	}

	if len(*resultExpr) > 0 {
		return resultExpr
	} else {
		return nil
	}
}

func (p exprParserImpl) parseProjectionExpression(expr string) *string {
	expr = strings.Trim(expr, " ")

	placeholderExpr := ""

	for len(expr) > 0 {
		name, remainder := p.parseName(expr)
		expr = remainder
		placeholderExpr = placeholderExpr + name

		if len(expr) > 0 {
			expr = strings.TrimLeft(expr[1:], " ")
			placeholderExpr = placeholderExpr + ","
		}
	}

	if len(placeholderExpr) > 0 {
		return &placeholderExpr
	} else {
		return nil
	}
}

// When names are nested, they need to be placeholdered separately
// When they're indexed, the index should be included raw (i.e. not in the placeholder value)
// e.g. a.b[1].c needs to become #1.#2[1].#3
func (p exprParserImpl) parseName(expr string) (resultName string, remainder string) {
	isEscaped := false
	if strings.HasPrefix(expr, "`") {
		isEscaped = true
		expr = expr[1:]
	}

	var parsedName string
	if isEscaped { // if escaped, just read until closing tilde
		indexEnd := strings.Index(expr, "`")
		parsedName = expr[:indexEnd]
		remainder = expr[indexEnd+1:]
	} else {
		parsedName, remainder = parseNextToken(expr, ".", "[")
	}

	resultName = p.addName(parsedName)

	if len(remainder) > 0 && remainder[0] == '[' {
		closingIdx := findWithOffset(remainder, "]", 1)
		resultName = resultName + remainder[:closingIdx+1]
		remainder = remainder[closingIdx+1:]
	}

	if len(remainder) > 0 && remainder[0] == '.' {
		parsedName, remainder = p.parseName(remainder[1:])
		resultName = resultName + "." + parsedName
	}

	return resultName, remainder
}

// returns the next token, as terminated by either a special or the end of the string
// the returned remainder starts with the termination string
func parseNextToken(expr string, additionalDelimiters ...string) (attributeName string, remainder string) {
	delimiterIndexes := []int{len(expr)}

	for _, s := range append(specials, additionalDelimiters...) {
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

func tryParseValue(expr string) (*dynamodb.AttributeValue, string, error) {
	var value *dynamodb.AttributeValue

	value, remainder, err := tryParseSimpleValue(expr)
	if err != nil {
		value, remainder, err = tryParseList(expr)
	}
	if err != nil {
		value, remainder, err = tryParseMap(expr)
	}
	if err != nil {
		return nil, expr, errors.New("Could not parse value at: " + expr)
	}

	return value, remainder, nil
}

func tryParseSimpleValue(expr string) (*dynamodb.AttributeValue, string, error) {
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
		return nil, expr, err
	}

	attributeValue, err := dynamodbattribute.Marshal(value)
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
		panic("Unterminated string: " + expr)
	}

	idxEscape := strings.Index(expr, "\\")

	for idxEscape > -1 && idxEscape < idxQuote {
		if expr[idxEscape+1] == '"' || expr[idxEscape+1] == '\'' || expr[idxEscape+1] == '\\' {
			expr = expr[:idxEscape] + expr[idxEscape+1:]
			idxQuote = findWithOffset(expr, "'", idxEscape+1)
			idxEscape = findWithOffset(expr, "\\", idxEscape+1)
		} else {
			panic("Unexpected escape character: " + expr)
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

	return nil, expr, errors.New("Expected number value at: " + expr)
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

	return false, expr, errors.New("Expected bool value at: " + expr)
}

func tryParseNull(expr string) (remainder string, err error) {
	nextToken, expr := parseNextToken(expr)
	if strings.EqualFold(nextToken, "null") {
		return expr, nil
	}

	return expr, errors.New("Expected null value at: " + expr)
}

func tryParseList(expr string) (list *dynamodb.AttributeValue, remainder string, err error) {
	if strings.HasPrefix(expr, "[") {
		expr = expr[1:]
	} else if strings.HasPrefix(expr, "<<") {
		expr = expr[2:]
	} else {
		return nil, expr, errors.New("Expected list value at: " + expr)
	}

	expr = strings.TrimLeft(expr, " ,")

	items := []*dynamodb.AttributeValue{}
	for !strings.HasPrefix(expr, "]") && !strings.HasPrefix(expr, ">>") {
		val, remainder, err := tryParseValue(expr)

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
		return nil, expr, errors.New("Expected map value at: " + expr)
	}

	expr = strings.TrimLeft(expr, " ")

	root := make(map[string]*dynamodb.AttributeValue)
	for !strings.HasPrefix(expr, "}") {
		colonIdx := findWithOffset(expr, ":", 0)

		name := strings.Trim(expr[0:colonIdx], " ")
		expr = strings.TrimLeft(expr[colonIdx+1:], " ")

		val, remainder, err := tryParseValue(expr)
		if err != nil {
			panic(err)
		}

		expr = strings.TrimLeft(remainder, " ,")
		root[name] = val
	}

	attributeValue := dynamodb.AttributeValue{M: root}
	return &attributeValue, expr[1:], nil
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
