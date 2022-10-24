Data Types
=====================

Scalar Types
--------------------------------------

Cider scalar datatypes, sizes and usage notes are described in the following table:


==========  ==================  ========================================================================
Type        Size(bytes)         Notes
==========  ==================  ========================================================================
BOOLEAN     1                   boolean values true and false.
TINYINT     1                   Minimum value of -2^7 and  maximum value of 2^7 - 1.
SMALLINT    2                   Minimum value of -2^15 and maximum value of 2^15 - 1.
INTEGER     4                   Minimum value of -2^31 and maximum value of 2^31 - 1.
BIGINT      8                   Minimum value of -2^63 and maximum value of 2^63 - 1.
FLOAT       4                   single precision floating point value. Minimum value: -3.4e38, Maximum value: 3.4e38.
DOUBLE      8                   double precision floating-point value. Minimum value: -1.79e308, Maximum value: 1.79e308.
TIME        8                   the value is in units of microseconds since the beginning of any day,
                                supported formats: HH:mm:ss[.SSS][XXX].
DATE        8                   the value is epoch from 1970-01-01 in seconds,supported formats:yyyy-mm-dd.
                                if you want to convert to days elapsed, you need to divide 24*60*60.
TIMESTAMP   8                   Mixed Date and Time Value, supported formats: yyyy-MM-dd HH:mm:ss[.SSS][XXX]
==========  ==================  ========================================================================
