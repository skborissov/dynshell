package main

import (
	"testing"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/require"
)

func Test_query_keyOnly(t *testing.T) {
	// given
	expectedKeyCondition := "#0 = :0 AND #1>=:1"

	expectedName0 := "pk"
	expectedName1 := "sk"
	expectedNames := map[string]*string{"#0": &expectedName0, "#1": &expectedName1}

	expectedValueStr := "someStr"
	expectedValue0 := dynamodb.AttributeValue{S: &expectedValueStr}
	expectedValueNumber := "123"
	expectedValue1 := dynamodb.AttributeValue{N: &expectedValueNumber}
	expectedValues := map[string]*dynamodb.AttributeValue{":0": &expectedValue0, ":1": &expectedValue1}

	// when
	var expr params = parseQuery("pk = 'someStr' AND sk>=123", "", "")

	// then
	require.Equal(t, expectedKeyCondition, *expr.keyExpr())
	require.Equal(t, expectedNames, expr.getNames())
	require.Equal(t, expectedValues, expr.getValues())
	require.Nil(t, expr.filterExpr())
	require.Nil(t, expr.projExpr())
}

func Test_query_complexNames(t *testing.T) {
	// given
	expectedFilterCondition := "#0.#1=:0 AND #2.#3[2] > :1 OR :2 = #4.#5[2].#6"

	expectedName0 := "a"
	expectedName1 := "b"
	expectedName2 := "c"
	expectedName3 := "d"
	expectedName4 := "e"
	expectedName5 := "f"
	expectedName6 := "g"
	expectedNames := map[string]*string{"#0": &expectedName0, "#1": &expectedName1, "#2": &expectedName2, "#3": &expectedName3, "#4": &expectedName4, "#5": &expectedName5, "#6": &expectedName6}

	expectedValue0Number := "1"
	expectedValue0 := dynamodb.AttributeValue{N: &expectedValue0Number}
	expectedValue1Number := "2"
	expectedValue1 := dynamodb.AttributeValue{N: &expectedValue1Number}
	expectedValue2Number := "3"
	expectedValue2 := dynamodb.AttributeValue{N: &expectedValue2Number}
	expectedValues := map[string]*dynamodb.AttributeValue{":0": &expectedValue0, ":1": &expectedValue1, ":2": &expectedValue2}

	// when
	// TODO add case where complex name is last
	var expr params = parseQuery("", "a.b=1 AND c.d[2] > 2 OR 3 = e.f[2].g", "")

	// then
	require.Equal(t, expectedFilterCondition, *expr.filterExpr())
	require.Equal(t, expectedNames, expr.getNames())
	require.Equal(t, expectedValues, expr.getValues())
	require.Nil(t, expr.keyExpr())
	require.Nil(t, expr.projExpr())
}

func Test_query_keyAndProj(t *testing.T) {
	// given
	expectedKeyCondition := "#0 = :0"

	expectedName0 := "pk"
	expectedName1 := "pk"
	expectedName2 := "field0"
	expectedNames := map[string]*string{"#0": &expectedName0, "#1": &expectedName1, "#2": &expectedName2}

	expectedValueStr := "someStr"
	expectedValue := dynamodb.AttributeValue{S: &expectedValueStr}
	expectedValues := map[string]*dynamodb.AttributeValue{":0": &expectedValue}

	expectedProj := "#1,#2"

	// when
	var expr params = parseQuery("pk = 'someStr'", "", "pk, field0")

	// then
	require.Equal(t, expectedKeyCondition, *expr.keyExpr())
	require.Equal(t, expectedNames, expr.getNames())
	require.Equal(t, expectedValues, expr.getValues())
	require.Equal(t, expectedProj, *expr.projExpr())
	require.Nil(t, expr.filterExpr())
}

func Test_query_filterOnly(t *testing.T) {
	// given
	expectedFilterCondition := "#0 = :0"

	expectedName := "field0"
	expectedNames := map[string]*string{"#0": &expectedName}

	expectedValueStr := "someStr"
	expectedValue := dynamodb.AttributeValue{S: &expectedValueStr}
	expectedValues := map[string]*dynamodb.AttributeValue{":0": &expectedValue}

	// when
	var expr params = parseQuery("", "field0 = 'someStr'", "")

	// then
	require.Equal(t, expectedFilterCondition, *expr.filterExpr())
	require.Equal(t, expectedNames, expr.getNames())
	require.Equal(t, expectedValues, expr.getValues())
	require.Nil(t, expr.keyExpr())
	require.Nil(t, expr.projExpr())
}

func Test_query_filterAndProj(t *testing.T) {
	// given
	expectedFilterCondition := "#0 = :0"

	expectedName1 := "field1"
	expectedName2 := "field2"
	expectedName3 := "field3"
	expectedNames := map[string]*string{"#0": &expectedName1, "#1": &expectedName2, "#2": &expectedName3}

	expectedValueStr := "someStr"
	expectedValue := dynamodb.AttributeValue{S: &expectedValueStr}
	expectedValues := map[string]*dynamodb.AttributeValue{":0": &expectedValue}

	expectedProj := "#1,#2"

	// when
	var expr params = parseQuery("", "field1 = 'someStr'", "field2,field3")

	// then
	require.Equal(t, expectedFilterCondition, *expr.filterExpr())
	require.Equal(t, expectedNames, expr.getNames())
	require.Equal(t, expectedValues, expr.getValues())
	require.Nil(t, expr.keyExpr())
	require.Equal(t, *expr.projExpr(), expectedProj)
}

func Test_query_keyAndFilterAndProj(t *testing.T) {
	// given
	expectedKeyCondition := "#0 = :0"
	expectedFilterCondition := "#1 = :1 AND #2 = :2"

	expectedName0 := "pk"
	expectedName1 := "field0"
	expectedName2 := "field1"
	expectedName3 := "pk"
	expectedName4 := "aValue"
	expectedNames := map[string]*string{"#0": &expectedName0, "#1": &expectedName1, "#2": &expectedName2, "#3": &expectedName3, "#4": &expectedName4}

	expectedValue0Str := "someStr"
	expectedValue0 := dynamodb.AttributeValue{S: &expectedValue0Str}
	expectedValue1Str := "aValue"
	expectedValue1 := dynamodb.AttributeValue{S: &expectedValue1Str}
	expectedValue2Str := "anotherValue"
	expectedValue2 := dynamodb.AttributeValue{S: &expectedValue2Str}
	expectedValues := map[string]*dynamodb.AttributeValue{":0": &expectedValue0, ":1": &expectedValue1, ":2": &expectedValue2}

	expectedProj := "#3,#4"

	// when
	var expr params = parseQuery("pk = 'someStr'", "field0 = 'aValue' AND field1 = 'anotherValue'", "pk,aValue")

	// then
	require.Equal(t, expectedKeyCondition, *expr.keyExpr())
	require.Equal(t, expectedNames, expr.getNames())
	require.Equal(t, expectedValues, expr.getValues())
	require.Equal(t, expectedFilterCondition, *expr.filterExpr())
	require.Equal(t, expectedProj, *expr.projExpr())
}

func Test_query_types(t *testing.T) {
	// given
	expectedFilterCondition := "#0 = :0 AND #1 = :1 AND #2 = :2 AND #3 = :3"

	expectedName0 := "field0"
	expectedName1 := "field1"
	expectedName2 := "field2"
	expectedName3 := "field3"
	expectedNames := map[string]*string{"#0": &expectedName0, "#1": &expectedName1, "#2": &expectedName2, "#3": &expectedName3}

	expectedValueStr := "someStr"
	expectedValue0 := dynamodb.AttributeValue{S: &expectedValueStr}
	expectedValueInt := "10"
	expectedValue1 := dynamodb.AttributeValue{N: &expectedValueInt}
	expectedValueFloat := "10.12345"
	expectedValue2 := dynamodb.AttributeValue{N: &expectedValueFloat}
	expectedValueBool := true
	expectedValue3 := dynamodb.AttributeValue{BOOL: &expectedValueBool}
	expectedValues := map[string]*dynamodb.AttributeValue{":0": &expectedValue0, ":1": &expectedValue1, ":2": &expectedValue2, ":3": &expectedValue3}

	// when
	var expr params = parseQuery("", "field0 = 'someStr' AND field1 = 10 AND field2 = 10.12345 AND field3 = true", "")

	// then
	require.Equal(t, expectedFilterCondition, *expr.filterExpr())
	require.Equal(t, expectedNames, expr.getNames())
	require.Equal(t, expectedValues, expr.getValues())
	require.Nil(t, expr.keyExpr())
	require.Nil(t, expr.projExpr())
}

func Test_query_list(t *testing.T) {
	// given
	expectedFilterCondition := "#0 = :0"

	expectedName0 := "field0"
	expectedNames := map[string]*string{"#0": &expectedName0}

	expectedValueStr := "someStr"
	expectedValue1 := dynamodb.AttributeValue{S: &expectedValueStr}

	expectedValueInt := "10"
	expectedValue2 := dynamodb.AttributeValue{N: &expectedValueInt}

	expectedValue := dynamodb.AttributeValue{L: []*dynamodb.AttributeValue{&expectedValue1, &expectedValue2}}

	// when
	var expr params = parseQuery("", "field0 = ['someStr', 10]", "")

	// then
	require.Equal(t, expectedFilterCondition, *expr.filterExpr())
	require.Equal(t, expectedNames, expr.getNames())

	require.Equal(t, 1, len(expr.getValues()))
	require.Equal(t, expectedValue, *expr.getValues()[":0"])
	require.Nil(t, expr.keyExpr())
	require.Nil(t, expr.projExpr())

}

func Test_query_sets(t *testing.T) {
	// given
	expectedFilterCondition := "#0 = :0 AND #1 = :1"

	expectedName0 := "field0"
	expectedName1 := "field1"
	expectedNames := map[string]*string{"#0": &expectedName0, "#1": &expectedName1}

	expectedValueStr0 := "someStr0"
	expectedValueStr1 := "someStr1"
	expectedValueNum0 := "123"
	expectedValueNum1 := "456.789"

	expectedStringSet := dynamodb.AttributeValue{SS: []*string{&expectedValueStr0, &expectedValueStr1}}
	expectedNumberSet := dynamodb.AttributeValue{NS: []*string{&expectedValueNum0, &expectedValueNum1}}

	// when
	var expr params = parseQuery("", "field0 = <<'someStr0','someStr1'>> AND field1 = << 123 , 456.789  >>", "")

	// then
	require.Equal(t, expectedFilterCondition, *expr.filterExpr())
	require.Equal(t, expectedNames, expr.getNames())

	require.Equal(t, 2, len(expr.getValues()))
	require.Equal(t, expectedStringSet, *expr.getValues()[":0"])
	require.Equal(t, expectedNumberSet, *expr.getValues()[":1"])
	require.Nil(t, expr.keyExpr())
	require.Nil(t, expr.projExpr())
}

func Test_query_map(t *testing.T) {
	// given
	expectedFilterCondition := "#0 = :0"

	expectedName0 := "field0"
	expectedNames := map[string]*string{"#0": &expectedName0}

	expectedValueStr0 := "mapValue0"
	expectedValue0 := dynamodb.AttributeValue{S: &expectedValueStr0}
	expectedValueNum0 := "123"
	expectedValue1 := dynamodb.AttributeValue{N: &expectedValueNum0}

	expectedMap := dynamodb.AttributeValue{M: map[string]*dynamodb.AttributeValue{"mapField0": &expectedValue0, "mapField1": &expectedValue1}}

	// when
	var expr params = parseQuery("", "field0 = { mapField0: 'mapValue0', mapField1: 123 }", "")

	// then
	require.Equal(t, expectedFilterCondition, *expr.filterExpr())
	require.Equal(t, expectedNames, expr.getNames())

	require.Equal(t, 1, len(expr.getValues()))
	require.Equal(t, expectedMap, *expr.getValues()[":0"])
	require.Nil(t, expr.keyExpr())
	require.Nil(t, expr.projExpr())
}

func Test_query_specialTokens(t *testing.T) {
	// given
	expectedKeyCondition := "((#0 = :0) AND NOT (#1 = :1))"
	expectedKey0 := "pk"
	expectedKey1 := "sk"
	expectedFilterCondition := "NOT #2 > :2 OR #3 < :3"
	expectedField0 := "field0"
	expectedField1 := "field1"
	expectedNames := map[string]*string{"#0": &expectedKey0, "#1": &expectedKey1, "#2": &expectedField0, "#3": &expectedField1}
	expectedValueStr0 := "1000"
	expectedValue0 := dynamodb.AttributeValue{N: &expectedValueStr0}
	expectedValueStr1 := "123.222"
	expectedValue1 := dynamodb.AttributeValue{N: &expectedValueStr1}
	expectedValueStr2 := "str1"
	expectedValue2 := dynamodb.AttributeValue{S: &expectedValueStr2}
	expectedValueStr3 := "15"
	expectedValue3 := dynamodb.AttributeValue{N: &expectedValueStr3}
	expectedValues := map[string]*dynamodb.AttributeValue{":0": &expectedValue0, ":1": &expectedValue1, ":2": &expectedValue2, ":3": &expectedValue3}

	// when
	var expr params = parseQuery("((pk = 1000) AND NOT (sk = 123.222))", "NOT field0 > 'str1' OR field1 < 15", "")

	// then
	require.Equal(t, expectedKeyCondition, *expr.keyExpr())
	require.Equal(t, expectedFilterCondition, *expr.filterExpr())
	require.Equal(t, expectedNames, expr.getNames())
	require.Equal(t, expectedValues, expr.getValues())
	require.Nil(t, expr.projExpr())
}

func Test_query_comparators(t *testing.T) {
	// given
	expectedKeyCondition := "#0 = :0"
	expectedKey0 := "pk"
	expectedFilterCondition := "#1 > :1 OR #2 < :2 OR #3 >= :3 OR #4 <= :4 OR #5 <> :5"
	expectedField0 := "field0"
	expectedField1 := "field1"
	expectedField2 := "field2"
	expectedField3 := "field3"
	expectedField4 := "field4"
	expectedNames := map[string]*string{"#0": &expectedKey0, "#1": &expectedField0, "#2": &expectedField1, "#3": &expectedField2, "#4": &expectedField3, "#5": &expectedField4}

	expectedValueStr0 := "1000"
	expectedValue0 := dynamodb.AttributeValue{N: &expectedValueStr0}
	expectedValueStr1 := "str1"
	expectedValue1 := dynamodb.AttributeValue{S: &expectedValueStr1}
	expectedValueStr2 := "15"
	expectedValue2 := dynamodb.AttributeValue{N: &expectedValueStr2}
	expectedValueStr3 := "10"
	expectedValue3 := dynamodb.AttributeValue{N: &expectedValueStr3}
	expectedValueStr4 := "11"
	expectedValue4 := dynamodb.AttributeValue{N: &expectedValueStr4}
	expectedValueStr5 := "12"
	expectedValue5 := dynamodb.AttributeValue{N: &expectedValueStr5}
	expectedValues := map[string]*dynamodb.AttributeValue{":0": &expectedValue0, ":1": &expectedValue1, ":2": &expectedValue2, ":3": &expectedValue3, ":4": &expectedValue4, ":5": &expectedValue5}

	// when
	var expr params = parseQuery("pk = 1000", "field0 > 'str1' OR field1 < 15 OR field2 >= 10 OR field3 <= 11 OR field4 <> 12", "")

	// then
	require.Equal(t, expectedKeyCondition, *expr.keyExpr())
	require.Equal(t, expectedFilterCondition, *expr.filterExpr())
	require.Equal(t, expectedNames, expr.getNames())
	require.Equal(t, expectedValues, expr.getValues())
	require.Nil(t, expr.projExpr())
}

func Test_query_between(t *testing.T) {
	// given
	expectedFilterCondition := "#0 BETWEEN :0 AND :1"

	expectedName0 := "field0"
	expectedNames := map[string]*string{"#0": &expectedName0}

	expectedValue0Str := "value0"
	expectedValue0 := dynamodb.AttributeValue{S: &expectedValue0Str}
	expectedValue1Str := "value1"
	expectedValue1 := dynamodb.AttributeValue{S: &expectedValue1Str}
	expectedValues := map[string]*dynamodb.AttributeValue{":0": &expectedValue0, ":1": &expectedValue1}

	// when
	var expr params = parseQuery("", "field0 BETWEEN 'value0' AND 'value1'", "")

	require.Equal(t, expectedFilterCondition, *expr.filterExpr())
	require.Equal(t, expectedNames, expr.getNames())
	require.Equal(t, expectedValues, expr.getValues())
	require.Nil(t, expr.keyExpr())
	require.Nil(t, expr.projExpr())
}

func Test_query_in(t *testing.T) {
	// given
	expectedFilterCondition := "#0 IN (:0, :1, :2)"

	expectedName0 := "field1"
	expectedNames := map[string]*string{"#0": &expectedName0}

	expectedValue0Str := "value0"
	expectedValue0 := dynamodb.AttributeValue{S: &expectedValue0Str}
	expectedValue1Str := "value1"
	expectedValue1 := dynamodb.AttributeValue{S: &expectedValue1Str}
	expectedValue2Str := "value2"
	expectedValue2 := dynamodb.AttributeValue{S: &expectedValue2Str}
	expectedValues := map[string]*dynamodb.AttributeValue{":0": &expectedValue0, ":1": &expectedValue1, ":2": &expectedValue2}

	// when
	var expr params = parseQuery("", "field1 IN ('value0', 'value1', 'value2')", "")

	require.Equal(t, expectedFilterCondition, *expr.filterExpr())
	require.Equal(t, expectedNames, expr.getNames())
	require.Equal(t, expectedValues, expr.getValues())
	require.Nil(t, expr.keyExpr())
	require.Nil(t, expr.projExpr())
}

func Test_query_functions(t *testing.T) {
	// given
	expectedFilterCondition := "begins_with(#0, :0) OR attribute_exists(#1) OR attribute_not_exists(#2) OR attribute_type(#3, :1) OR contains(#4, :2)"

	expectedName0 := "field0"
	expectedName1 := "field1"
	expectedName2 := "field2"
	expectedName3 := "field3"
	expectedName4 := "field4"
	expectedNames := map[string]*string{"#0": &expectedName0, "#1": &expectedName1, "#2": &expectedName2, "#3": &expectedName3, "#4": &expectedName4}

	expectedValue0Str := "someStr0"
	expectedValue0 := dynamodb.AttributeValue{S: &expectedValue0Str}
	expectedValue1Str := "S"
	expectedValue1 := dynamodb.AttributeValue{S: &expectedValue1Str}
	expectedValue2Str := "someStr1"
	expectedValue2 := dynamodb.AttributeValue{S: &expectedValue2Str}
	expectedValues := map[string]*dynamodb.AttributeValue{":0": &expectedValue0, ":1": &expectedValue1, ":2": &expectedValue2}

	// when
	var expr params = parseQuery("", "begins_with(field0, 'someStr0') OR attribute_exists(field1) OR attribute_not_exists(field2) OR attribute_type(field3, 'S') OR contains(field4, 'someStr1')", "")

	// then
	require.Equal(t, expectedFilterCondition, *expr.filterExpr())
	require.Equal(t, expectedNames, expr.getNames())
	require.Equal(t, expectedValues, expr.getValues())
	require.Nil(t, expr.keyExpr())
	require.Nil(t, expr.projExpr())
}

func Test_query_extraSpaces(t *testing.T) {
	// given
	expectedFilterCondition := "   #0   IN  ( :0,   :1,:2  ) AND   begins_with(   #1  ,  :3   )  AND #2   =   :4"

	expectedName0 := "field0"
	expectedName1 := "field1"
	expectedName2 := "field2"
	expectedNames := map[string]*string{"#0": &expectedName0, "#1": &expectedName1, "#2": &expectedName2}

	expectedValue0Str := "value0"
	expectedValue0 := dynamodb.AttributeValue{S: &expectedValue0Str}
	expectedValue1Str := "value1"
	expectedValue1 := dynamodb.AttributeValue{S: &expectedValue1Str}
	expectedValue2Str := "value2"
	expectedValue2 := dynamodb.AttributeValue{S: &expectedValue2Str}
	expectedValue3Str := "value3"
	expectedValue3 := dynamodb.AttributeValue{S: &expectedValue3Str}
	expectedValue4Num := "123"
	expectedValue4 := dynamodb.AttributeValue{N: &expectedValue4Num}
	expectedValues := map[string]*dynamodb.AttributeValue{":0": &expectedValue0, ":1": &expectedValue1, ":2": &expectedValue2, ":3": &expectedValue3, ":4": &expectedValue4}

	// when
	var expr params = parseQuery("", "   field0   IN  ( 'value0',   'value1','value2'  ) AND   begins_with(   field1  ,  'value3'   )  AND field2   =   123", "")

	require.Equal(t, expectedFilterCondition, *expr.filterExpr())
	require.Equal(t, expectedNames, expr.getNames())
	require.Equal(t, expectedValues, expr.getValues())
	require.Nil(t, expr.keyExpr())
	require.Nil(t, expr.projExpr())
}

func Test_query_projection(t *testing.T) {
	empty := parseQuery("pk = 1000", "", "   ")
	single := parseQuery("pk = 1000", "", "a")
	multiple := parseQuery("pk = 1000", "", "a,b,c")
	multipleWithSpaces := parseQuery("pk = 1000", "", "a, b,c")
	complexNames := parseQuery("pk = 1000", "", "a.b[0].c, d.e, f.g[0]")

	require.Nil(t, empty.projExpr())

	require.Equal(t, *single.projExpr(), "#1")
	require.Equal(t, *single.getNames()["#0"], "pk")
	require.Equal(t, *single.getNames()["#1"], "a")

	require.Equal(t, *multiple.projExpr(), "#1,#2,#3")
	require.Equal(t, *multiple.getNames()["#0"], "pk")
	require.Equal(t, *multiple.getNames()["#1"], "a")
	require.Equal(t, *multiple.getNames()["#2"], "b")
	require.Equal(t, *multiple.getNames()["#3"], "c")

	require.Equal(t, *multipleWithSpaces.projExpr(), "#1,#2,#3")
	require.Equal(t, *multiple.getNames()["#0"], "pk")
	require.Equal(t, *multiple.getNames()["#1"], "a")
	require.Equal(t, *multiple.getNames()["#2"], "b")
	require.Equal(t, *multiple.getNames()["#3"], "c")

	require.Equal(t, *complexNames.projExpr(), "#1.#2[0].#3,#4.#5,#6.#7[0]")
}

func Test_types_stringEscaping(t *testing.T) {
	escapedSlashes := parseQuery("pk = 'someStr\\\\'", "", "")
	escapedSingleQuotes := parseQuery("pk = 'It\\'s an apostrophe'", "", "")
	escapedDoubleQuotes := parseQuery("pk = '\\\"Something\\\", he said'", "", "")

	require.Equal(t, "someStr\\", *escapedSlashes.getValues()[":0"].S)
	require.Equal(t, "It's an apostrophe", *escapedSingleQuotes.getValues()[":0"].S)
	require.Equal(t, "\"Something\", he said", *escapedDoubleQuotes.getValues()[":0"].S)
}

func Test_names_nameEscaping(t *testing.T) {
	escapedKeyword := parseQuery("`size` = 123", "", "")
	escapedNumber := parseQuery("`123` = 'abcd'", "", "")
	escapedComplexName := parseQuery("`a`.b[3].`c d`.`e`[2] = 'abcd'", "", "")

	require.Equal(t, "size", *escapedKeyword.getNames()["#0"])
	require.Equal(t, "123", *escapedNumber.getNames()["#0"])

	require.Equal(t, "a", *escapedComplexName.getNames()["#0"])
	require.Equal(t, "b", *escapedComplexName.getNames()["#1"])
	require.Equal(t, "c d", *escapedComplexName.getNames()["#2"])
	require.Equal(t, "e", *escapedComplexName.getNames()["#3"])
	require.Equal(t, "#0.#1[3].#2.#3[2] = :0", *escapedComplexName.keyExpr())
}
