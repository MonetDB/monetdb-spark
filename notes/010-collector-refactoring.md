# Collector refactoring

`org.monetdb.spark.workerside.Collector` is monetdb-spark's implementation of
monetdb-java's `org.monetdb.jdbc.MonetConnection.UploadHandler`. It contains a
collection of numbered ByteArrayOutputStreams, typically one per column. During
conversion, every appender appends bytes to its own output stream and during the
upload phase, the server asks for their contents.

We use ByteArrayOutputStreams because that seems to be the only growable append
buffer in the Java standard library.

This worked fine until implemented compression. We did the compression during
the upload so the Collector could keep holding ByteArrayOutputStreams. However,
we also want to add support for [back references] in string columns. It would
really be better to do the encoding while collecting, not while uploading.

Another consideration is memory use. The ByteArrayOutputStreams get reset after
each batch is uploaded, but they keep their largest allocation, even if later
batches don't need it. It would be nice to have a Collector-wide storage pool
instead of per-column.

Also, currently the steps are initialized with a reference to their
OutputStream which they keep forever. If we have compression and
multiple batches, the output streams need to be replaced.



[back references]: https://www.monetdb.org/documentation-Dec2025/user-guide/sql-manual/data-loading/binary-loading/#encoding-repeated-strings

## Step 1: abstract the appending interface

Collector hands out CollectorStream, a final subclass of BufferedOutputStream.
This means the write method will inline nicely. It allows the collector to
replace the underlying stream.


## Step 2: CollectorStream wraps the compression algorithm

So we do it while flushing the buffer, not while sending it.

After each batch the Collector replaces the inner streams, appenders just
keep their reference to their CollectorStream.


## Step 3: The String appenders start to do backref encoding

It's not part of the collector.


## Optional step: SharedBufferStream

