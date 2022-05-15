package main

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/c-bata/go-prompt"
	"github.com/jessevdk/go-flags"
)

type tableContext struct {
	name           string
	hashAttribute  string
	rangeAttribute string
	indexes        []string
	allTables      []*string
}

func createDynamo(endpointUrl *string, region *string) *dynamodb.DynamoDB {
	session := session.Must(session.NewSession(&aws.Config{
		Endpoint: endpointUrl,
		Region:   region,
	}))

	return dynamodb.New(session)
}

var opts struct {
	// TODO get region from aws config?
	EndpointUrl string `long:"endpoint-url" description:"Override the default URL with a given URL"`
	Region      string `long:"region" description:"The region to use" required:"true"`
	Verbose     bool   `short:"v" long:"verbose" description:"Verbose output"`
}

func main() {
	_, err := flags.Parse(&opts)
	if err != nil {
		panic(err)
	}

	dynamo := createDynamo(&opts.EndpointUrl, &opts.Region)
	listTablesOutput, _ := dynamo.ListTables(&dynamodb.ListTablesInput{})
	tableCtx := tableContext{
		allTables: listTablesOutput.TableNames,
	}

	livePrefix := func() (prefix string, live bool) {
		promptPrefix := *dynamo.Config.Region
		if tableCtx.name != "" {
			promptPrefix += ":" + tableCtx.name
		}
		promptPrefix += "> "
		return promptPrefix, true
	}

	p := prompt.New(
		newExecutor(dynamo, &tableCtx).execute,
		newCompleter(&tableCtx).complete,
		prompt.OptionTitle("dynshell"),
		prompt.OptionLivePrefix(livePrefix),
		prompt.OptionAddKeyBind(prompt.KeyBind{
			Key: prompt.ControlLeft,
			Fn:  prompt.GoLeftWord,
		}),
		prompt.OptionAddKeyBind(prompt.KeyBind{
			Key: prompt.ControlRight,
			Fn:  prompt.GoRightWord,
		}),
		prompt.OptionCompletionWordSeparator("{- \""),
	)

	p.Run()
}
