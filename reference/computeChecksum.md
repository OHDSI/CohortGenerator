# Computes the checksum for a value

This is used as part of the incremental operations to hash a value to
store in a record keeping file. This function leverages the md5 hash
from the digest package

## Usage

``` r
computeChecksum(val)
```

## Arguments

- val:

  The value to hash. It is converted to a character to perform the hash.

## Value

Returns a string containing the checksum
