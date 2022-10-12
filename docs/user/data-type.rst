Data Types
=====================

Numeric type
--------------------------------------

TINYINT
++++++++++++++++++++++++++++++++++++++
A 8-bit signed two’s complement integer with a minimum value of -2^7 and a maximum value of 2^7 - 1.

SMALLINT
++++++++++++++++++++++++++++++++++++++
A 16-bit signed two’s complement integer with a minimum value of -2^15 and a maximum value of 2^15 - 1.

INTEGER
++++++++++++++++++++++++++++++++++++++
A 32-bit signed two’s complement integer with a minimum value of -2^31 and a maximum value of 2^31 - 1.

BIGINT
++++++++++++++++++++++++++++++++++++++
A 64-bit signed two’s complement integer with a minimum value of -2^63 and a maximum value of 2^63 - 1.

FLOAT
++++++++++++++++++++++++++++++++++++++
A real is a 32-bit inexact, variable-precision implementing the IEEE Standard 754 for Binary Floating-Point Arithmetic.

DOUBLE
++++++++++++++++++++++++++++++++++++++
A double is a 64-bit inexact, variable-precision implementing the IEEE Standard 754 for Binary Floating-Point Arithmetic.

Boolean type
--------------------------------------
BOOLEAN
++++++++++++++++++++++++++++++++++++++
This type captures boolean values true and false.


Datetime type
--------------------------------------
DATE
++++++++++++++++++++++++++++++++++++++
A date specifies a combination of year, month and day, the 64-bit representation of the value is  epoch from 1970-01-01 in seconds, if you want to
convert to days elapsed, you need to divide 24*60*60.

TIME
++++++++++++++++++++++++++++++++++++++
A time specifies a combination of hour, minute, second, millisecond and microsecond, the 64-bit representation of the value is in units of microseconds since the beginning of any day.

TIMESTAMP
++++++++++++++++++++++++++++++++++++++
A timestamp specifies a combination of DATE (year, month, day) and a TIME (hour, minute, second,  millisecond, microsecond). the 64-bit representation of the value is in units of microseconds since the UNIX epoch.



String type
--------------------------------------