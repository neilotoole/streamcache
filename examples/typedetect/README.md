# typedetect

`typedetect` is a trivial program that reads input from `stdin` or a specified
file, attempts to determine its file type (`json` and `xml` are
supported), and prints the file type, and a preview (head and tail) of the
file contents. It demonstrates use of the [`neilotoole/streamcache`](https://github.com/neilotoole/streamcache)
Go package.

## Usage

```shell
$ go install github.com/neilotoole/streamcache/examples/typedetect

$ typedetect testdata/data.json
# Or:
$ cat testdata/data.xml | typedetect
```

![streamcache_typedetect.png](streamcache_typedetect.png)
