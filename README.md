# samplereader: arcane cached reader

Package `samplereader` addresses an arcane scenario: multiple readers
want to sample the start of an input stream (from an `io.Reader`),
which involves caching, but after the samplers are satisfied,
there's no need to maintain that cache and its memory overhead.

Package `samplereader` implements a reader mechanism that allows
multiple callers to sample some or all of the of the contents of a
source reader, while only reading from the source reader once.

This is, admittedly, a rather arcane situation. But, here it is:

Let's say we're reading from `stdin`. For example:
 
```shell
$ cat myfile.ext | myprogram  
```

In this scenario, `myprogram` wants to detect the type of data
in the file/pipe, and then print it out. That sampling could be done
in a separate goroutine per sampler type. The input file could be,
let's say, a CSV file, or a TSV file.

The obvious approach is to inspect the first few lines of the
input, and check if the input is either valid CSV, or valid TSV.
After that process, let's say we want to dump out the entire contents
of the input.

Package `samplereader` provides a facility to create a `Source` from an
underlying `io.Reader` (`os.Stdin` in this scenario), and spawn multiple
readers, each of which can operate independently, in their own
goroutines if desired. The underlying source (again, `os.Stdin` in this
scenario) will only once be read from, but its data is available to
multiple readers, because that data is cached in memory.

That is, until there's only one final reader left, (after invoking
`Source.Seal`) at which point the cache is discarded, and
the final reader reads straight from the underlying source.


## Related / Similar Projects
- [github.com/djherbis/fscache](https://github.com/djherbis/fscache)
