# Dynshell
A more user-friendly DynamoDB experience as compared to the AWS CLI

- Simplifies writing expressions by removing the need for DynamoDB JSON and placeholders, while preserving expression syntax.
- Autocompletion for table names, keys, flag names and values
- Command history and keyboard shortcuts as provided by [go-prompt](https://github.com/c-bata/go-prompt)

![render1650993139634](https://user-images.githubusercontent.com/75425111/165355827-f0a4783d-624c-499d-b038-6370177adf35.gif)

## Setup
### Requirements
- AWS CLI configured
### Installation
Binaries can be found on [Github releases](https://github.com/skborissov/dynshell/releases). To build locally, run `go build`.
## Usage
### Available commands
* `use`    Change table context
* `desc`   Describe current table
* `query`  Based on AWS CLI [query](https://docs.aws.amazon.com/cli/latest/reference/dynamodb/query.html)
* `scan`   Based on AWS CLI [scan](https://docs.aws.amazon.com/cli/latest/reference/dynamodb/scan.html)
* `update` Based on AWS CLI [update-item](https://docs.aws.amazon.com/cli/latest/reference/dynamodb/update-item.html)
* `put`    Based on AWS CLI [put-item](https://docs.aws.amazon.com/cli/latest/reference/dynamodb/put-item.html)
* `delete` Based on AWS CLI [delete-item](https://docs.aws.amazon.com/cli/latest/reference/dynamodb/delete-item.html)
### Expression syntax
Expressions syntax is simplified in how attribute names and values are provided, but is otherwise unchanged.
#### Names
Names are written literally. Exceptions to this are special names (a small subset of the DynamoDB [reserved names](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ReservedWords.html) and names with spaces, which should be quoted with backticks (\`).
#### Values
The way values are handled is based on DynamoDB's [PartiQL support](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ql-reference.data-types.html). Examples for each supported type are listed below.
* `Boolean`    true
* `Number`     123.456
* `String`     'string value' (single quotes can be escaped with a backslash (\))
* `Null`       NULL
* `Number Set` <<1, 2.5, 3>>
* `String Set` <<'first', 'second', 'third'>>
* `List`       [123, 'string']
* `Map`        { key1: 'value1', key2: 123 }
