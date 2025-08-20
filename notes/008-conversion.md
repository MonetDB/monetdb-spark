# Conversion refactoring

So far we have a number of Converter classes which call the appropriate
getter on a `Row` object and append bytes to a buffer. However, we
changed the Dialect to map, for example, ByteType to SMALLINT so we need
to be able to support size changes. This means we have a much larger
number of combinations so we must do some refactoring.

The basic design is as follows: we split the Converter class into
Extractor, Converter and Appender.

We have an Extractor for each Spark type. The Extractor knows which Row
getter to call. The Extractor leaves the value in a scratch field of the
Collector. Size conversions are cheap so we'll probably store all
int-like types in a long and float and double both in a double.
There will also be a wasNull flag.

We have an Appender for each MonetDB type. It will take the value of
of the appropriate scratch field and append it to the column's buffer.
It will first check the wasNull flag and write the appropriate null
pattern if it was set.

In some cases, more work is needed. For example, for integer types a
range check is needed unless the destination type is strictly wider. For
COPY BINARY, some not too wide Spark Decimals can we written using an
Integer appender but the integer needs to be extracted from the Decimal
object. For these operations we have converters. Converters read and
write the scratch fields.


## Overflows

What happens if a value does not fit the destination type?
We'll offer an option "overflowIsNull". **TODO better name?**
The default will be false to alert the user to the existence of
overflows. If set to true, overflows will silently be replaced by NULLs.


## Integer conversions

All integer extractors plus the Boolean extractor will leave the
result in `scratchLong`. The appenders can simply take the appropriate
slice of that long and append it to their buffer.

There is a single RangeCheck converter with a parameter N.
It rejects everything > N and < -N.


## Decimals

We still only support uploads where the scale doesn't change.

The extractors leave the unscaled value in `scratchLong`.
We can then use the integer appenders and range check.
However, the range check will be something like 9999 instead of
32767.


## Huge Decimals

There are actually three cases.

1. Huge Spark decimal to huge MonetDB decimal.

2. Huge Spark decimal to regular MonetDB decimal.

3. Regular Spark decimal to huge MonetDB decimal.


We write our own HugeInt class containing two longs.
It has the bare minimum needed operations:
FromDecimal, fromLong, lessThan and negate. To implement negate
we need invert and increment.

In Case 1 we use a HugeDecimalExtractor to extract a HugeInt into
`scratchHugeInt`. If needed we perform a HugeRangeCheck.
Then we use the HugeIntAppender.

Case 2 is the same, however, instead of the HugeIntAppender we use
a HugeIntToLong converter and the regular LongAppender.

In Case 3 we use the regular DecimalExtractor, a LongToHugeInt converter
and the HugeIntAppender.


## Floats

We have a FloatExtractor and a DoubleExtractor and a FloatAppender
and a DoubleAppender. The intermediate result is always `scratchDouble`.


## Non-interval temporal types


## Interval types
