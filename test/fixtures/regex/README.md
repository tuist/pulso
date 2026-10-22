# Bounded regex controls

`go-controls.json` records 221 `regexp.MatchString` answers from the standard
library in Go 1.25.10 (`darwin/arm64`), generated with `produce.go.txt`.
It exercises ASCII Perl classes, Unicode case folds (Kelvin sign and long s),
vertical tab, codepoint dot, strict dollar anchors, word boundaries and literal
carets/hyphens following word-complement classes and literal closing-brace
repetition. The original 80 answers were retained
unchanged when these edge controls were added.
Ordinary Elixir tests replay these answers against the independent PCRE
reference and the native byte matcher; no Go installation or network is needed.
This small corpus is not full RE2 or malformed-syntax conformance.

To regenerate deliberately, copy the producer to a `.go` file and run
`GOTOOLCHAIN=local GOPROXY=off GOSUMDB=off go run <producer.go>`.
Review differences instead of silently refreshing expected answers.
