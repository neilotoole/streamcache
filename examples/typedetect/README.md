# typedetect

`typedetect` is a trivial program that reads input from `stdin` or a specified
file, attempts to determine its file type (`json`, `xml`, `html`, and `go` are
supported), and prints the file type, and a preview (head and tail) of the
file contents. It demonstrates use of the [`neilotoole/streamcache`](https://github.com/neilotoole/streamcache)
Go package.

## Usage

```shell
$ go install github.com/neilotoole/streamcache/examples/typedetect

# Or:
$ typedetect data.json
$ cat data.json | typedetect
```

