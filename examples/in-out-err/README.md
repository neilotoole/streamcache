# in-out-err

`in-out-err` is a trivial example program that copies the contents of stdin
to stdout and stderr, and prints the number of bytes read. It exists to
demonstrate the use of [`neilotoole/streamcache`](https://github.com/neilotoole/streamcache).

## Usage

```shell
$ go install github.com/neilotoole/streamcache/examples/in-out-err

$ echo "hello world" | in-out-err
hello world
hello world
Read 12 bytes from stdin
```

Note that the `in-out-err` code ignores most errors; don't do that in real life.
See the [`multicase`](../multicase) example for a more complete program.
