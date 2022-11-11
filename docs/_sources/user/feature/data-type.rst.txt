Data Types
=====================

Scalar Types
--------------------------------------

Cider scalar datatypes, sizes and usage notes are described in the following table:


==========  ========================        ===========================================================================================
SQL Type    Arrow data type                 Notes
==========  ========================        ===========================================================================================
BOOLEAN     boolean                         boolean values true and false.
TINYINT     int8                            Minimum value of -2^7 and  maximum value of 2^7 - 1.
SMALLINT    int16                           Minimum value of -2^15 and maximum value of 2^15 - 1.
INTEGER     int32                           Minimum value of -2^31 and maximum value of 2^31 - 1.
BIGINT      int64                           Minimum value of -2^63 and maximum value of 2^63 - 1.
FLOAT       float32                         single precision floating point value. Minimum value: -3.4e38, Maximum value: 3.4e38.
DOUBLE      float64                         double precision floating-point value. Minimum value: -1.79e308, Maximum value: 1.79e308.
TIME        time64 [microseconds]           the value is in units of microseconds since the beginning of any day,
                                            supported formats: HH:mm:ss[.SSS][XXX].
DATE        date32 [days]                   days since 1970-01-01,supported formats:yyyy-mm-dd.
TIMESTAMP   timestamp [microseconds]        Mixed Date and Time Value, supported formats: yyyy-MM-dd HH:mm:ss[.SSS][XXX]
==========  ========================        ===========================================================================================
