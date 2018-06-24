# StreamServer
This is a Maven project as part of the stream processing simulation experiments. StreamServer simulates a stream join processing engine running as a command-line application, where it receives a number of input streams from remote sources and join them based on a common attribute. StreamServer includes several time-based window stream join algorithms:
- [MJoin](https://www.sciencedirect.com/science/article/pii/B9780127224428500331)
- [AMJoin](https://search.ieice.org/bin/summary.php?id=e92-d_7_1429)
- [Time-slide window join (with its variants)](https://link.springer.com/article/10.1007/s10844-014-0325-4)

## Requirements
- Java
- Stream sources (datasets) with `key` and `value` atributes, `key` will be used as the join attribute

## How to use
- Build the project (e.g. build a `StreamServer.jar`) 
- Run the `StreamServer.jar` within a terminal, e.g. by issuing this command:
```
java -jar StreamServer.jar -p 9999 -j mjoin -s rstream.txt:sstream.txt:tstream.txt:ustream.txt
```
The command above will run StreamServer by opening a port number `9999` on the host running it and is expecting four stream sources: `rstream.txt`, `sstream.txt`, `tstream.txt`, and `ustream.txt` to be joined using `MJoin` algorithm. 

## Command-line arguments
Below are the complete list of arguments accepted by this application:
- `p`, specify the host port number, if not specified `4444` is used as the default value
- `s`, specify the expected streams to be joined separated by colon `:`, e.g. `-s rstream.txt:sstream.txt` makes the command-line will only read on streams named `rstream.txt` and `sstream.txt`. This option also specify the default probing order of the join operation, e.g. `-s rstream.txt:sstream.txt:tstream.txt:ustream.txt` will create a join order `rstream.txt-sstream.txt-tstream.txt-ustream.txt`. If a data arrives from `rstream.txt` then it will probe `sstream.txt -> tstream.txt -> ustream.txt`. Likewise, if the data arrives from `sstream.txt` the probing order will become `rstream.txt -> tstream.txt -> ustream.txt`. This option is mandatory and has no default value.
- `j`, specify the join algorithm to join the streams, if not specified `MJoin` would be used as the join algorithm. The acceptable values for this option are: `mjoin`, `amjoin`, `tswa4ojoin`, `tswa4mjoin`, `tswa5join`, `tswa6join`, and `tswa7join`.
- `W`, specify the window size (in seconds), if not specified `4` is used as the default value
- `T`, specify the window slide time (in seconds), if not specified `2` is used as the default value. This option is only affecting Time-slide window join algorithm variants.
- `Te`, specify the join evaluation time (in seconds if evaluated by time or in number of tuples if evaluated by amount of tuple that has been receieved), if not specified `1` is used as the dafault value. This option is only affecting Time-slide window join algorithm variants.
- `t`, specify the execution duration for StreamServer (in seconds), if not specified `120` is used as the default value
