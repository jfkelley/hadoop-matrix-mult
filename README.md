## README for optimized sparse matrix multiplication on Hadoop

This utility follows the unix philosophy of doing exactly one thing and doing it well: sparse matrix multiplication on Hadoop.

A full explanation of the algorithm involved is available on my website at: [http://www.joefkelley.com/?p=853](http://www.joefkelley.com/?p=853)

The implementation is mostly in Scala, with some small Java portions for performance-sensitive sections (i.e., in-memory submatrix multiplication).

### Building

I use sbt (and sbt-assembly plugin) to build. Run:

`sbt assembly`

and it will generate `target/scala-2.11/hadoop-matrix-mult-assembly-1.1.jar`

The current targetted version of Hadoop is 2.4.0 (HDP 2.1) but there's nothing fancy being used, so other versions should work fine as well.

### Running

After building the jar, simply push it to your hadoop cluster, and run:

`hadoop jar hadoop-matrix-mult-assembly-1.1.jar [hadoop specific args] [program specific args]`

The program-specific arguments are:

Argument | Description | Example
--- | --- | --- | ---
`--left [path]` | The path containing the input matrix on the left side of the multiplication | `--input hdfs:///user/hadoop/matrix_A`
`--right [path]` | The path containing the input matrix on the right side of the multiplication | `--output hdfs:///user/hadoop/matrix_B`
`--output [path]` | The directory to store the output into | `--output hdfs:///user/hadoop/matrix_C`
`--nDivs [int]` | The number of submatrixes to divide each input matrix into | `--nDivs 20`
`--oneMR` | If present, executes the job in a single MR pass instead of two. Requires nDivs^2 = number of reduce tasks | `--oneMR`
`--inputConverter` | The class (extending com.joefkelley.matrix.mr.LineToMatrixElement) to use for converting input lines into matrix elements | `--inputConverter my.custom.LineToMatrixElement`
`--outputConverter` | The class (extending com.joefkelley.matrix.mr.MatrixElementToLine) to use for converting matrix elements to output lines | `--outputConverter my.custom.MatrixElementToLine`

The argument `--nDivs` is a bit tricky to explain; my [blog post](http://www.joefkelley.com/?p=853) has the full details of its implications. But the take-aways are this:

* nDivs^3 should be greater than the number of reduce tasks, which should be greater than the number of reduce slots.
* 1/nDivs^2 of each matrix needs to fit into the reduce tasks' memory at once.
* nDivs should be set as low as possible while still meeting those requirements.

An example invocation would be:

`hadoop jar hadoop-matrix-mult_2.11-1.1.jar -D mapred.reduce.tasks=1000 --left hdfs:///my/input/A --right hdfs:///my/input/B --output hdfs:///my/output --nDivs 20`

Note that setting `mapred.reduce.tasks` is important, as the default is 1. If you use `--oneMR`, whatever you set will be overridden to nDivs^2.


#### Input/Output Formats

The expected input format (and output format) is simple tab-separated value files with the format:

`<row> [tab] <column> [tab] <value>`

Note that values for row and column need not be ints. They can be any unique id. The output matrix will have rows equal to the left input matrix's rows, and columns equal to the right input matrix's columns, where values are summed along matching left-column-ids and right-row-ids. This is useful if the rows/columns have a semantic meaning and it's easier to express indexes as the natural id's of whatever the rows and columns represent instead of assigning a contiguous integer to each.

Of course, if it is simpler not to think about it that way, regular integer indexes will work just fine too.

If your input data is not TSV, but is some other text-file-based format, you can provide custom input/output converters: essentially functions `Line => (Row, Col, Value)` and `(Row, Col, Value) => Line`. Your classes should extend `com.joefkelley.matrix.mr.LineToMatrixElement` and `com.joefkelley.matrix.mr.MatrixElementToLine`, respectively. The only methods to override have signatures:

`def setRowColValue(line: Text, row: Text, col: Text, value: DoubleWritable): Unit`

and

`def setLine(row: Text, col: Text, value: DoubleWritable, line: Text): Unit`

The base traits extend Configured, so you can also access the MR job's configuration via `getConf()`.

Then simply provide the fully-qualified names of those classes to the respective command-line args.


### Planned future improvements

* Allow even-more configurable input/output formats (non-text-based)
* Intelligently calculate nDivs by default. This could maybe be done by inspecting reducer memory settings and doing some calculations based on that and the total input data volume.
* Investigate whether Strassen's algorithm or other matrix multiplication algorithms would actually speed up submatrix multiplication.
* Improve the push of phase-2 combiner/reducer logic into phase-1's reduce so that it can be done partially and does not require special combination of settings or using only a single MR job.

### Change Log

#### 1.2
* Added custom input/output converters

#### 1.1
* Switched from maven to sbt
* Fixed custom partitioner
* Added `--oneMR` option

#### 1.0
* Initial release

### License

Do whatever you want with it. Copy it, modify it, release it as part of something proprietary; I don't really care. Just maybe be nice and give me a shout-out if you like it :)

If you want the legalese version, here's the one I chose to copy+paste:

<hr>

This is free and unencumbered software released into the public domain.

Anyone is free to copy, modify, publish, use, compile, sell, or
distribute this software, either in source code form or as a compiled
binary, for any purpose, commercial or non-commercial, and by any
means.

In jurisdictions that recognize copyright laws, the author or authors
of this software dedicate any and all copyright interest in the
software to the public domain. We make this dedication for the benefit
of the public at large and to the detriment of our heirs and
successors. We intend this dedication to be an overt act of
relinquishment in perpetuity of all present and future rights to this
software under copyright law.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
OTHER DEALINGS IN THE SOFTWARE.

For more information, please refer to <http://unlicense.org>