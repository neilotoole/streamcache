module github.com/neilotoole/streamcache

// Minimum version 1.20 because that's when context.Cause was introduced.
go 1.21

toolchain go1.21.6

require (
	github.com/neilotoole/fifomu v0.1.1
	github.com/stretchr/testify v1.8.4
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
