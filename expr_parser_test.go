package main

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/require"
)

func Test_query_keyOnly(t *testing.T) {
	// given
	expectedKeyCondition := "#0 = :0 AND #1>=:1"

	expectedNames := map[string]*string{
		"#0": name("pk"),
		"#1": name("sk"),
	}

	expectedValues := map[string]*dynamodb.AttributeValue{
		":0": str("someStr"),
		":1": integer(123),
	}

	// when
	exprParser := newExprParser()
	expr := exprParser.parseGenericExpression("pk = 'someStr' AND sk>=123")

	// then
	require.Equal(t, expectedKeyCondition, *expr)
	require.Equal(t, expectedNames, exprParser.getNames())
	require.Equal(t, expectedValues, exprParser.getValues())
}

func Test_query_complexNames(t *testing.T) {
	// given
	expectedFilterCondition := "#0.#1=:0 AND #2.#3[2] > :1 OR :2 = #4.#5[2].#6"

	expectedNames := map[string]*string{
		"#0": name("a"),
		"#1": name("b"),
		"#2": name("c"),
		"#3": name("d"),
		"#4": name("e"),
		"#5": name("f"),
		"#6": name("g"),
	}

	expectedValues := map[string]*dynamodb.AttributeValue{
		":0": integer(1),
		":1": integer(2),
		":2": integer(3),
	}

	// when
	exprParser := newExprParser()
	expr := exprParser.parseGenericExpression("a.b=1 AND c.d[2] > 2 OR 3 = e.f[2].g")

	// then
	require.Equal(t, expectedFilterCondition, *expr)
	require.Equal(t, expectedNames, exprParser.getNames())
	require.Equal(t, expectedValues, exprParser.getValues())
}

func Test_query_keyAndProj(t *testing.T) {
	// given
	expectedKeyCondition := "#0 = :0"

	expectedNames := map[string]*string{
		"#0": name("pk"),
		"#1": name("pk"),
		"#2": name("field0"),
	}

	expectedValues := map[string]*dynamodb.AttributeValue{
		":0": str("someStr"),
	}

	expectedProj := "#1,#2"

	// when
	exprParser := newExprParser()
	expr := exprParser.parseGenericExpression("pk = 'someStr'")
	proj := exprParser.parseProjectionExpression("pk, field0")

	// then
	require.Equal(t, expectedKeyCondition, *expr)
	require.Equal(t, expectedProj, *proj)
	require.Equal(t, expectedNames, exprParser.getNames())
	require.Equal(t, expectedValues, exprParser.getValues())
}

func Test_query_filterOnly(t *testing.T) {
	// given
	expectedFilterCondition := "#0 = :0"

	expectedNames := map[string]*string{"#0": name("field0")}

	expectedValues := map[string]*dynamodb.AttributeValue{
		":0": str("someStr"),
	}

	// when
	exprParser := newExprParser()
	expr := exprParser.parseGenericExpression("field0 = 'someStr'")

	// then
	require.Equal(t, expectedFilterCondition, *expr)
	require.Equal(t, expectedNames, exprParser.getNames())
	require.Equal(t, expectedValues, exprParser.getValues())
}

func Test_query_filterAndProj(t *testing.T) {
	// given
	expectedFilterCondition := "#0 = :0"

	expectedNames := map[string]*string{
		"#0": name("field1"),
		"#1": name("field2"),
		"#2": name("field3"),
	}

	expectedValues := map[string]*dynamodb.AttributeValue{
		":0": str("someStr"),
	}

	expectedProj := "#1,#2"

	// when
	exprParser := newExprParser()
	expr := exprParser.parseGenericExpression("field1 = 'someStr'")
	proj := exprParser.parseProjectionExpression("field2,field3")

	// then
	require.Equal(t, expectedFilterCondition, *expr)
	require.Equal(t, expectedProj, *proj)
	require.Equal(t, expectedNames, exprParser.getNames())
	require.Equal(t, expectedValues, exprParser.getValues())
}

func Test_query_keyAndFilterAndProj(t *testing.T) {
	// given
	expectedKeyCondition := "#0 = :0"
	expectedFilterCondition := "#1 = :1 AND #2 = :2"

	expectedNames := map[string]*string{
		"#0": name("pk"),
		"#1": name("field0"),
		"#2": name("field1"),
		"#3": name("pk"),
		"#4": name("aValue"),
	}

	expectedValues := map[string]*dynamodb.AttributeValue{
		":0": str("someStr"),
		":1": str("aValue"),
		":2": str("anotherValue"),
	}

	expectedProj := "#3,#4"

	// when
	exprParser := newExprParser()
	key := exprParser.parseGenericExpression("pk = 'someStr'")
	filter := exprParser.parseGenericExpression("field0 = 'aValue' AND field1 = 'anotherValue'")
	proj := exprParser.parseProjectionExpression("pk,aValue")

	// then
	require.Equal(t, expectedKeyCondition, *key)
	require.Equal(t, expectedFilterCondition, *filter)
	require.Equal(t, expectedProj, *proj)
	require.Equal(t, expectedNames, exprParser.getNames())
	require.Equal(t, expectedValues, exprParser.getValues())
}

func Test_query_types(t *testing.T) {
	// given
	expectedFilterCondition := "#0 = :0 AND #1 = :1 AND #2 = :2 AND #3 = :3"

	expectedNames := map[string]*string{
		"#0": name("field0"),
		"#1": name("field1"),
		"#2": name("field2"),
		"#3": name("field3"),
	}

	expectedValues := map[string]*dynamodb.AttributeValue{
		":0": str("someStr"),
		":1": integer(10),
		":2": float(10.12345),
		":3": boolean(true),
	}

	// when
	exprParser := newExprParser()
	expr := exprParser.parseGenericExpression("field0 = 'someStr' AND field1 = 10 AND field2 = 10.12345 AND field3 = true")

	// then
	require.Equal(t, expectedFilterCondition, *expr)
	require.Equal(t, expectedNames, exprParser.getNames())
	require.Equal(t, expectedValues, exprParser.getValues())
}

func Test_query_list(t *testing.T) {
	// given
	expectedFilterCondition := "#0 = :0"

	expectedNames := map[string]*string{
		"#0": name("field0"),
	}

	expectedValue := dynamodb.AttributeValue{L: []*dynamodb.AttributeValue{str("someStr"), integer(10)}}

	// when
	exprParser := newExprParser()
	expr := exprParser.parseGenericExpression("field0 = ['someStr', 10]")

	// then
	require.Equal(t, expectedFilterCondition, *expr)
	require.Equal(t, expectedNames, exprParser.getNames())

	require.Equal(t, 1, len(exprParser.getValues()))
	require.Equal(t, expectedValue, *exprParser.getValues()[":0"])

}

func Test_query_sets(t *testing.T) {
	// given
	expectedFilterCondition := "#0 = :0 AND #1 = :1"

	expectedNames := map[string]*string{
		"#0": name("field0"),
		"#1": name("field1"),
	}

	expectedStringSet := stringSet([]string{"someStr0", "someStr1"})
	expectedNumberSet := numberSet([]string{"123", "456.789"})

	// when
	exprParser := newExprParser()
	expr := exprParser.parseGenericExpression("field0 = <<'someStr0','someStr1'>> AND field1 = << 123 , 456.789  >>")

	// then
	require.Equal(t, expectedFilterCondition, *expr)
	require.Equal(t, expectedNames, exprParser.getNames())

	require.Equal(t, 2, len(exprParser.getValues()))
	require.Equal(t, *expectedStringSet, *exprParser.getValues()[":0"])
	require.Equal(t, *expectedNumberSet, *exprParser.getValues()[":1"])
}

func Test_query_map(t *testing.T) {
	// given
	expectedFilterCondition := "#0 = :0"

	expectedNames := map[string]*string{
		"#0": name("field0"),
	}

	expectedMap := dynamodb.AttributeValue{
		M: map[string]*dynamodb.AttributeValue{
			"mapField0": str("mapValue0"),
			"mapField1": integer(123),
		},
	}

	// when
	exprParser := newExprParser()
	expr := exprParser.parseGenericExpression("field0 = { mapField0: 'mapValue0', mapField1: 123 }")

	// then
	require.Equal(t, expectedFilterCondition, *expr)
	require.Equal(t, expectedNames, exprParser.getNames())

	require.Equal(t, 1, len(exprParser.getValues()))
	require.Equal(t, expectedMap, *exprParser.getValues()[":0"])
}

func Test_query_specialTokens(t *testing.T) {
	// given
	expectedKeyCondition := "((#0 = :0) AND NOT (#1 = :1))"
	expectedFilterCondition := "NOT #2 > :2 OR #3 < :3"

	expectedNames := map[string]*string{
		"#0": name("pk"),
		"#1": name("sk"),
		"#2": name("field0"),
		"#3": name("field1"),
	}

	expectedValues := map[string]*dynamodb.AttributeValue{
		":0": integer(1000),
		":1": float(123.222),
		":2": str("str1"),
		":3": integer(15),
	}

	// when
	exprParser := newExprParser()
	key := exprParser.parseGenericExpression("((pk = 1000) AND NOT (sk = 123.222))")
	filter := exprParser.parseGenericExpression("NOT field0 > 'str1' OR field1 < 15")

	// then
	require.Equal(t, expectedKeyCondition, *key)
	require.Equal(t, expectedFilterCondition, *filter)
	require.Equal(t, expectedNames, exprParser.getNames())
	require.Equal(t, expectedValues, exprParser.getValues())
}

func Test_query_comparators(t *testing.T) {
	// given
	expectedKeyCondition := "#0 = :0"
	expectedFilterCondition := "#1 > :1 OR #2 < :2 OR #3 >= :3 OR #4 <= :4 OR #5 <> :5"
	expectedNames := map[string]*string{
		"#0": name("pk"),
		"#1": name("field0"),
		"#2": name("field1"),
		"#3": name("field2"),
		"#4": name("field3"),
		"#5": name("field4"),
	}

	expectedValues := map[string]*dynamodb.AttributeValue{
		":0": integer(1000),
		":1": str("str1"),
		":2": integer(15),
		":3": integer(10),
		":4": integer(11),
		":5": integer(12),
	}

	// when
	exprParser := newExprParser()
	key := exprParser.parseGenericExpression("pk = 1000")
	filter := exprParser.parseGenericExpression("field0 > 'str1' OR field1 < 15 OR field2 >= 10 OR field3 <= 11 OR field4 <> 12")

	// then
	require.Equal(t, expectedKeyCondition, *key)
	require.Equal(t, expectedFilterCondition, *filter)
	require.Equal(t, expectedNames, exprParser.getNames())
	require.Equal(t, expectedValues, exprParser.getValues())
}

func Test_query_between(t *testing.T) {
	// given
	expectedFilterCondition := "#0 BETWEEN :0 AND :1"

	expectedNames := map[string]*string{
		"#0": name("field0"),
	}

	expectedValues := map[string]*dynamodb.AttributeValue{
		":0": str("value0"),
		":1": str("value1"),
	}

	// when
	exprParser := newExprParser()
	expr := exprParser.parseGenericExpression("field0 BETWEEN 'value0' AND 'value1'")

	require.Equal(t, expectedFilterCondition, *expr)
	require.Equal(t, expectedNames, exprParser.getNames())
	require.Equal(t, expectedValues, exprParser.getValues())
}

func Test_query_in(t *testing.T) {
	// given
	expectedFilterCondition := "#0 IN (:0, :1, :2)"

	expectedNames := map[string]*string{
		"#0": name("field1"),
	}

	expectedValues := map[string]*dynamodb.AttributeValue{
		":0": str("value0"),
		":1": str("value1"),
		":2": str("value2"),
	}

	// when
	exprParser := newExprParser()
	expr := exprParser.parseGenericExpression("field1 IN ('value0', 'value1', 'value2')")

	require.Equal(t, expectedFilterCondition, *expr)
	require.Equal(t, expectedNames, exprParser.getNames())
	require.Equal(t, expectedValues, exprParser.getValues())
}

func Test_query_functions(t *testing.T) {
	// given
	expectedFilterCondition := "begins_with(#0, :0) OR attribute_exists(#1) OR attribute_not_exists(#2) OR attribute_type(#3, :1) OR contains(#4, :2)"

	expectedNames := map[string]*string{
		"#0": name("field0"),
		"#1": name("field1"),
		"#2": name("field2"),
		"#3": name("field3"),
		"#4": name("field4"),
	}

	expectedValues := map[string]*dynamodb.AttributeValue{
		":0": str("someStr0"),
		":1": str("S"),
		":2": str("someStr1"),
	}

	// when
	exprParser := newExprParser()
	expr := exprParser.parseGenericExpression("begins_with(field0, 'someStr0') OR attribute_exists(field1) OR attribute_not_exists(field2) OR attribute_type(field3, 'S') OR contains(field4, 'someStr1')")

	// then
	require.Equal(t, expectedFilterCondition, *expr)
	require.Equal(t, expectedNames, exprParser.getNames())
	require.Equal(t, expectedValues, exprParser.getValues())
}

func Test_query_extraSpaces(t *testing.T) {
	// given
	expectedFilterCondition := "   #0   IN  ( :0,   :1,:2  ) AND   begins_with(   #1  ,  :3   )  AND #2   =   :4"

	expectedNames := map[string]*string{
		"#0": name("field0"),
		"#1": name("field1"),
		"#2": name("field2"),
	}

	expectedValues := map[string]*dynamodb.AttributeValue{
		":0": str("value0"),
		":1": str("value1"),
		":2": str("value2"),
		":3": str("value3"),
		":4": integer(123),
	}

	// when
	exprParser := newExprParser()
	expr := exprParser.parseGenericExpression("   field0   IN  ( 'value0',   'value1','value2'  ) AND   begins_with(   field1  ,  'value3'   )  AND field2   =   123")

	require.Equal(t, expectedFilterCondition, *expr)
	require.Equal(t, expectedNames, exprParser.getNames())
	require.Equal(t, expectedValues, exprParser.getValues())
}

func Test_query_projection(t *testing.T) {
	exprParser := newExprParser()
	empty := exprParser.parseProjectionExpression("   ")
	require.Nil(t, empty)

	exprParser = newExprParser()
	single := exprParser.parseProjectionExpression("a")
	require.Equal(t, "#0", *single)
	require.Equal(t, "a", *exprParser.getNames()["#0"], "pk")

	exprParser = newExprParser()
	multiple := exprParser.parseProjectionExpression("a,b,c")
	require.Equal(t, "#0,#1,#2", *multiple)
	require.Equal(t, *exprParser.getNames()["#0"], "a")
	require.Equal(t, *exprParser.getNames()["#1"], "b")
	require.Equal(t, *exprParser.getNames()["#2"], "c")

	exprParser = newExprParser()
	multipleWithSpaces := exprParser.parseProjectionExpression("a, b,c")
	require.Equal(t, "#0,#1,#2", *multipleWithSpaces)
	require.Equal(t, *exprParser.getNames()["#0"], "a")
	require.Equal(t, *exprParser.getNames()["#1"], "b")
	require.Equal(t, *exprParser.getNames()["#2"], "c")

	exprParser = newExprParser()
	complexNames := exprParser.parseProjectionExpression("a.b[0].c, d.e, f.g[0]")
	require.Equal(t, "#0.#1[0].#2,#3.#4,#5.#6[0]", *complexNames)
}

func Test_types_stringEscaping(t *testing.T) {
	exprParser := newExprParser()

	exprParser.parseGenericExpression("pk = 'someStr\\\\'")
	exprParser.parseGenericExpression("pk = 'It\\'s an apostrophe'")
	exprParser.parseGenericExpression("pk = '\\\"Something\\\", he said'")

	require.Equal(t, "someStr\\", *exprParser.getValues()[":0"].S)
	require.Equal(t, "It's an apostrophe", *exprParser.getValues()[":1"].S)
	require.Equal(t, "\"Something\", he said", *exprParser.getValues()[":2"].S)
}

func Test_names_nameEscaping(t *testing.T) {
	exprParser := newExprParser()
	exprParser.parseGenericExpression("`size` = 123")
	require.Equal(t, "size", *exprParser.getNames()["#0"])

	exprParser = newExprParser()
	exprParser.parseGenericExpression("`123` = 'abcd'")
	require.Equal(t, "123", *exprParser.getNames()["#0"])

	exprParser = newExprParser()
	escapedComplexName := exprParser.parseGenericExpression("`a`.b[3].`c d`.`e`[2] = 'abcd'")
	require.Equal(t, "#0.#1[3].#2.#3[2] = :0", *escapedComplexName)
	require.Equal(t, "a", *exprParser.getNames()["#0"])
	require.Equal(t, "b", *exprParser.getNames()["#1"])
	require.Equal(t, "c d", *exprParser.getNames()["#2"])
	require.Equal(t, "e", *exprParser.getNames()["#3"])
}

func name(str string) *string {
	return &str
}

func str(str string) *dynamodb.AttributeValue {
	out := dynamodb.AttributeValue{S: &str}
	return &out
}

func integer(integer int) *dynamodb.AttributeValue {
	str := fmt.Sprint(integer)
	out := dynamodb.AttributeValue{N: &str}
	return &out
}

func float(float float64) *dynamodb.AttributeValue {
	str := fmt.Sprint(float)
	out := dynamodb.AttributeValue{N: &str}
	return &out
}

func boolean(boolean bool) *dynamodb.AttributeValue {
	out := dynamodb.AttributeValue{BOOL: &boolean}
	return &out
}

func stringSet(strings []string) *dynamodb.AttributeValue {
	pointers := []*string{}
	for i := range strings {
		pointers = append(pointers, &strings[i])

	}
	out := dynamodb.AttributeValue{SS: pointers}
	return &out
}

func numberSet(numbers []string) *dynamodb.AttributeValue {
	pointers := []*string{}
	for i := range numbers {
		pointers = append(pointers, &numbers[i])

	}
	out := dynamodb.AttributeValue{NS: pointers}
	return &out
}
