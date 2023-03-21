#ifndef NEXTGEN_FUNCTION_CIDERSTRINGFUNCTION_H
#define NEXTGEN_FUNCTION_CIDERSTRINGFUNCTION_H

#include <cstdint>

extern "C" int8_t convert_string_to_tinyint(const char* str_ptr, const int32_t str_len);

extern "C" int16_t convert_string_to_smallint(const char* str_ptr, const int32_t str_len);

extern "C" int32_t convert_string_to_int(const char* str_ptr, const int32_t str_len);

extern "C" int64_t convert_string_to_bigint(const char* str_ptr, const int32_t str_len);

extern "C" float convert_string_to_float(const char* str_ptr, const int32_t str_len);

extern "C" int8_t convert_string_to_bool(const char* str_ptr, const int32_t str_len);

extern "C" double convert_string_to_double(const char* str_ptr, const int32_t str_len);

extern "C" int32_t convert_string_to_date(const char* str_ptr, const int32_t str_len);

extern "C" int64_t convert_string_to_timestamp(const char* str_ptr,
                                               const int32_t str_len,
                                               const int32_t dim);

extern "C" int64_t convert_string_to_time(const char* str_ptr,
                                          const int32_t str_len,
                                          const int32_t dim);
#endif  // NEXTGEN_FUNCTION_CIDERSTRINGFUNCTION_H
