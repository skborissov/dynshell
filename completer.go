package main

import (
	"reflect"
	"regexp"
	"sort"
	"strings"

	"github.com/c-bata/go-prompt"
)

var commands []string = []string{"exit", "use", "desc", "query", "scan", "delete", "update"}

func newCompleter(tableCtx *TableContext) Completer {
	return Completer{tableCtx: tableCtx}
}

type Completer struct {
	tableCtx *TableContext
}

func (c *Completer) Complete(doc prompt.Document) []prompt.Suggest {
	if len(doc.CurrentLineBeforeCursor()) == 0 {
		return []prompt.Suggest{}
	}

	if len(strings.Split(doc.CurrentLineBeforeCursor(), " ")) == 1 && doc.CurrentLineBeforeCursor()[len(doc.CurrentLineBeforeCursor())-1] != ' ' {
		return c.completeCmd(doc)
	}

	cmd := strings.Split(doc.CurrentLineBeforeCursor(), " ")[0]

	switch cmd {
	case "use":
		return c.completeUse(doc)
	case "query":
		return c.completeQuery(doc)
	case "scan":
		return c.completeScan(doc)
	case "delete":
		return c.completeDelete(doc)
	default:
		return []prompt.Suggest{}
	}
}

func (c *Completer) completeCmd(doc prompt.Document) []prompt.Suggest {
	matches := []prompt.Suggest{}

	for _, cmd := range commands {
		if strings.HasPrefix(cmd, doc.CurrentLineBeforeCursor()) {
			matches = append(matches, prompt.Suggest{Text: cmd})
		}
	}

	// If there were no matches, return all commands
	if len(matches) == 0 {
		for _, cmd := range commands {
			matches = append(matches, prompt.Suggest{Text: cmd})
		}
	}

	return matches

}

func (c *Completer) completeUse(doc prompt.Document) []prompt.Suggest {
	words := strings.Split(doc.CurrentLineBeforeCursor(), " ")

	if len(words) > 2 {
		return []prompt.Suggest{}
	}

	if len(words) == 2 {
		inputTable := words[1]

		matches := []prompt.Suggest{}

		for _, table := range c.tableCtx.tableNames {
			isCurrentTable := c.tableCtx.name == *table

			if !isCurrentTable && strings.HasPrefix(*table, inputTable) {
				matches = append(matches, prompt.Suggest{Text: *table})
			}
		}

		return matches
	}

	matches := []prompt.Suggest{}

	for _, table := range c.tableCtx.tableNames {
		matches = append(matches, prompt.Suggest{Text: *table})
	}

	return matches
}

func (c *Completer) completeQuery(doc prompt.Document) (suggestions []prompt.Suggest) {
	matched, suggestions := c.completeKeyFirst(doc, true)
	if matched {
		return suggestions
	}

	matched, suggestions = c.completeKeySecond(doc, true)
	if matched {
		return suggestions
	}

	unusedFlags := getUnusedFlags(doc, &readOpts{})
	unusedFlags = append(unusedFlags, getUnusedFlags(doc, &queryOpts{})...)

	return c.completeRead(doc, unusedFlags)
}

func (c *Completer) completeScan(doc prompt.Document) (suggestions []prompt.Suggest) {
	unusedFlags := getUnusedFlags(doc, &readOpts{})
	unusedFlags = append(unusedFlags, getUnusedFlags(doc, &scanOpts{})...)

	return c.completeRead(doc, unusedFlags)
}

func (c *Completer) completeRead(doc prompt.Document, unusedFlags []flag) (suggestions []prompt.Suggest) {
	readFlags := getCmdFlags(&readOpts{})
	enumFlags := map[flag][]string{
		findFlagByShort(readFlags, "s"): {"ALL_ATTRIBUTES", "ALL_PROJECTED_ATTRIBUTES", "SPECIFIC_ATTRIBUTES", "COUNT"},
		findFlagByShort(readFlags, "r"): {"INDEXES", "TOTAL", "NONE"},
		findFlagByShort(readFlags, "i"): c.tableCtx.indexNames}

	return c.completeFlags(doc, unusedFlags, enumFlags)
}

func (c *Completer) completeDelete(doc prompt.Document) (suggestions []prompt.Suggest) {
	matched, suggestions := c.completeKeyFirst(doc, false)
	if matched {
		return suggestions
	}

	matched, suggestions = c.completeKeySecond(doc, false)
	if matched {
		return suggestions
	}

	deleteFlags := getCmdFlags(&deleteOpts{})
	enumFlags := map[flag][]string{
		findFlagByShort(deleteFlags, "r"): {"INDEXES", "TOTAL", "NONE"}}

	return c.completeFlags(doc, getUnusedFlags(doc, &deleteOpts{}), enumFlags)
}

func (c *Completer) completeFlags(doc prompt.Document, unusedFlags []flag, enumFlags map[flag][]string) (suggestions []prompt.Suggest) {
	if isInParameter(doc) {
		return []prompt.Suggest{}
	}

	matched, suggestions := completeFlag(doc, unusedFlags)
	if matched {
		return suggestions
	}

	matched, suggestions = completeEnum(doc, &enumFlags)
	if matched {
		return suggestions
	}

	return []prompt.Suggest{}
}

func completeFlag(doc prompt.Document, unusedFlags []flag) (matched bool, suggestions []prompt.Suggest) {
	var rgxFlag = regexp.MustCompile(`.* (-{1,2})([a-zA-Z]*)$`)
	allMatches := rgxFlag.FindAllStringSubmatch(doc.CurrentLineBeforeCursor(), -1)
	if len(allMatches) > 0 {
		matched = true

		matches := allMatches[len(allMatches)-1]

		flagInput := matches[2]

		for _, unusedFlag := range unusedFlags {
			if len(matches[1]) == 1 && unusedFlag.short != "" && strings.HasPrefix(unusedFlag.short, flagInput) {
				suggestions = append(suggestions, prompt.Suggest{Text: unusedFlag.short, Description: unusedFlag.desc})
			}
			if len(matches[1]) == 2 && strings.HasPrefix(unusedFlag.long, flagInput) {
				suggestions = append(suggestions, prompt.Suggest{Text: unusedFlag.long, Description: unusedFlag.desc})
			}
		}

		sort.Slice(suggestions, func(i, j int) bool {
			return suggestions[i].Text < suggestions[j].Text
		})
	}

	return matched, suggestions
}

func (c *Completer) completeKeyFirst(doc prompt.Document, isExpression bool) (matched bool, suggestions []prompt.Suggest) {
	var rgxKeyStart *regexp.Regexp
	if isExpression {
		rgxKeyStart = regexp.MustCompile(`.* (-k|--key) "(\s*)([a-zA-Z0-9_]*)$`)
	} else {
		rgxKeyStart = regexp.MustCompile(`.* (-k|--key) "(\s*){(\s*)([a-zA-Z0-9_]*)$`)
	}

	matches := rgxKeyStart.FindStringSubmatch(doc.CurrentLineBeforeCursor())
	if len(matches) > 0 {
		matched = true

		keyInput := matches[len(matches)-1]
		if strings.HasPrefix(c.tableCtx.hashAttributeName, keyInput) {
			suggestions = append(suggestions, prompt.Suggest{Text: c.tableCtx.hashAttributeName, Description: "pk"})
		}
		if c.tableCtx.rangeAttributeName != "" && strings.HasPrefix(c.tableCtx.rangeAttributeName, keyInput) {
			suggestions = append(suggestions, prompt.Suggest{Text: c.tableCtx.rangeAttributeName, Description: "sk"})
		}
	}

	return matched, suggestions
}

func (c *Completer) completeKeySecond(doc prompt.Document, isExpression bool) (matched bool, suggestions []prompt.Suggest) {
	var rgxKeyAnd *regexp.Regexp
	if isExpression {
		rgxKeyAnd = regexp.MustCompile(`.* (-k|--key) "(.*) AND(\s+)([a-zA-Z0-9_]*)$`)
	} else {
		rgxKeyAnd = regexp.MustCompile(`.* (-k|--key) "(\s*){(.*),(\s+)([a-zA-Z0-9_]*)$`)
	}

	matches := rgxKeyAnd.FindStringSubmatch(doc.CurrentLineBeforeCursor())

	if len(matches) > 0 {
		matched = true

		var firstCondition string
		if isExpression {
			firstCondition = matches[2]
		} else {
			firstCondition = matches[3]
		}

		keyInput := matches[len(matches)-1]
		if strings.HasPrefix(c.tableCtx.hashAttributeName, keyInput) {
			if !strings.Contains(firstCondition, c.tableCtx.hashAttributeName) {
				suggestions = append(suggestions, prompt.Suggest{Text: c.tableCtx.hashAttributeName, Description: "pk"})
			}
		}
		if c.tableCtx.rangeAttributeName != "" && strings.HasPrefix(c.tableCtx.rangeAttributeName, keyInput) {
			if !strings.Contains(firstCondition, c.tableCtx.rangeAttributeName) {
				suggestions = append(suggestions, prompt.Suggest{Text: c.tableCtx.rangeAttributeName, Description: "sk"})
			}
		}
	}

	return matched, suggestions
}

func completeEnum(doc prompt.Document, enumVals *map[flag][]string) (matched bool, suggestions []prompt.Suggest) {
	keys := make([]string, len(*enumVals)*2)

	i := 0
	for k := range *enumVals {
		keys[i] = k.short
		i++
		keys[i] = k.long
		i++
	}

	enumMatch := strings.Join(keys, "|")
	var rgxEnums = regexp.MustCompile(`(-{1,2})(` + enumMatch + `)(\s+)([A-Za-z]*)$`)

	allMatches := rgxEnums.FindAllStringSubmatch(doc.CurrentLineBeforeCursor(), -1)
	if len(allMatches) > 0 {
		matched = true

		match := allMatches[len(allMatches)-1]

		for k, v := range *enumVals {
			if (match[1] == "-" && match[2] == k.short) || (match[1] == "--" && match[2] == k.long) {
				enumInput := match[4]

				for _, value := range v {
					if strings.HasPrefix(value, enumInput) {
						suggestions = append(suggestions, prompt.Suggest{Text: value})
					}
				}
			}
		}
	}

	return matched, suggestions
}

func isInParameter(doc prompt.Document) (isInParameter bool) {
	for pos, char := range doc.CurrentLineBeforeCursor() {
		if char == '"' && doc.CurrentLineBeforeCursor()[pos-1] != '\\' {
			isInParameter = !isInParameter
		}
	}

	return isInParameter
}

type flag struct {
	short string
	long  string
	desc  string
}

func getUnusedFlags(doc prompt.Document, cmd interface{}) (unusedFlags []flag) {
	cmdFlags := getCmdFlags(cmd)
	params := parseArgs(doc.Text)

OUTER:
	for _, cmdFlag := range cmdFlags {
		for _, param := range params {
			if (len(param) > 2 && param[2:] == cmdFlag.long) || (len(param) == 2 && param[1:] == cmdFlag.short) {
				continue OUTER
			}
		}
		unusedFlags = append(unusedFlags, cmdFlag)
	}

	return unusedFlags
}

func getCmdFlags(cmd interface{}) (flags []flag) {
	t := reflect.TypeOf(cmd).Elem()

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)

		if field.Type.Kind() == reflect.Struct {
			continue
		} else {
			short := field.Tag.Get("short")
			long := field.Tag.Get("long")
			desc := field.Tag.Get("description")

			flags = append(flags, flag{short: short, long: long, desc: desc})
		}
	}

	return flags
}

func findFlagByShort(flags []flag, short string) (flag flag) {
	for _, f := range flags {
		if f.short == short {
			return f
		}
	}

	panic("Oops")
}
