#pragma once

#include <common/base/extended_types.h>
#include <common/base/strong_typedef.h>

namespace DB {
using UUID = StrongTypedef<UInt128, struct UUIDTag>;
}
