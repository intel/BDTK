add_library(_cityhash src/city.cc include/citycrc.h include/city.h)

target_include_directories(_cityhash SYSTEM BEFORE PUBLIC include)
target_include_directories(_cityhash SYSTEM PRIVATE src)
