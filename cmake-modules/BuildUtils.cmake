# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# \arg OUTPUTS list to append built targets to
function(ADD_ICL_LIB LIB_NAME)
  set(options BUILD_SHARED BUILD_STATIC)
  set(one_value_args CMAKE_PACKAGE_NAME PKG_CONFIG_NAME)
  set(multi_value_args
      SOURCES
      OUTPUTS
      STATIC_LINK_LIBS
      SHARED_LINK_LIBS
      SHARED_PRIVATE_LINK_LIBS
      EXTRA_INCLUDES
      PRIVATE_INCLUDES
      DEPENDENCIES
      SHARED_INSTALL_INTERFACE_LIBS
      STATIC_INSTALL_INTERFACE_LIBS
      OUTPUT_PATH)
  cmake_parse_arguments(ARG
                        "${options}"
                        "${one_value_args}"
                        "${multi_value_args}"
                        ${ARGN})
  if(ARG_UNPARSED_ARGUMENTS)
    message(SEND_ERROR "Error: unrecognized arguments: ${ARG_UNPARSED_ARGUMENTS}")
  endif()

  if(ARG_OUTPUTS)
    set(${ARG_OUTPUTS})
  endif()

  # Allow overriding ICL_BUILD_SHARED and ICL_BUILD_STATIC
  if(ARG_BUILD_SHARED)
    set(BUILD_SHARED ${ARG_BUILD_SHARED})
  else()
    set(BUILD_SHARED ${ICL_BUILD_SHARED})
  endif()
  if(ARG_BUILD_STATIC)
    set(BUILD_STATIC ${ARG_BUILD_STATIC})
  else()
    set(BUILD_STATIC ${ICL_BUILD_STATIC})
  endif()
  if(ARG_OUTPUT_PATH)
    set(OUTPUT_PATH ${ARG_OUTPUT_PATH})
  else()
    set(OUTPUT_PATH ${BUILD_OUTPUT_ROOT_DIRECTORY})
  endif()

  if(WIN32 OR (CMAKE_GENERATOR STREQUAL Xcode))
    # We need to compile C++ separately for each library kind (shared and static)
    # because of dllexport declarations on Windows.
    # The Xcode generator doesn't reliably work with Xcode as target names are not
    # guessed correctly.
    set(LIB_DEPS ${ARG_SOURCES})
    set(EXTRA_DEPS ${ARG_DEPENDENCIES})

    if(ARG_EXTRA_INCLUDES)
      set(LIB_INCLUDES ${ARG_EXTRA_INCLUDES})
    endif()
  else()
    # Otherwise, generate a single "objlib" from all C++ modules and link
    # that "objlib" into each library kind, to avoid compiling twice
    add_library(${LIB_NAME}_objlib OBJECT ${ARG_SOURCES})
    # Necessary to make static linking into other shared libraries work properly
    set_property(TARGET ${LIB_NAME}_objlib PROPERTY POSITION_INDEPENDENT_CODE 1)
    if(ARG_DEPENDENCIES)
      add_dependencies(${LIB_NAME}_objlib ${ARG_DEPENDENCIES})
    endif()
    set(LIB_DEPS $<TARGET_OBJECTS:${LIB_NAME}_objlib>)
    set(LIB_INCLUDES)
    set(EXTRA_DEPS)

    if(ARG_OUTPUTS)
      list(APPEND ${ARG_OUTPUTS} ${LIB_NAME}_objlib)
    endif()

    if(ARG_EXTRA_INCLUDES)
      target_include_directories(${LIB_NAME}_objlib SYSTEM PUBLIC ${ARG_EXTRA_INCLUDES})
    endif()
    if(ARG_PRIVATE_INCLUDES)
      target_include_directories(${LIB_NAME}_objlib PRIVATE ${ARG_PRIVATE_INCLUDES})
    endif()
  endif()

  set(RUNTIME_INSTALL_DIR bin)

  if(BUILD_SHARED)
    add_library(${LIB_NAME}_shared SHARED ${LIB_DEPS})
    if(EXTRA_DEPS)
      add_dependencies(${LIB_NAME}_shared ${EXTRA_DEPS})
    endif()

    if(ARG_OUTPUTS)
      list(APPEND ${ARG_OUTPUTS} ${LIB_NAME}_shared)
    endif()

    if(LIB_INCLUDES)
      target_include_directories(${LIB_NAME}_shared SYSTEM PUBLIC ${ARG_EXTRA_INCLUDES})
    endif()

    if(ARG_PRIVATE_INCLUDES)
      target_include_directories(${LIB_NAME}_shared PRIVATE ${ARG_PRIVATE_INCLUDES})
    endif()

    if(APPLE AND NOT DEFINED $ENV{EMSCRIPTEN})
      # On OS X, you can avoid linking at library load time and instead
      # expecting that the symbols have been loaded separately. This happens
      # with libpython* where there can be conflicts between system Python and
      # the Python from a thirdparty distribution
      #
      # When running with the Emscripten Compiler, we need not worry about
      # python, and the Emscripten Compiler does not support this option.
      set(ARG_SHARED_LINK_FLAGS "-undefined dynamic_lookup ${ARG_SHARED_LINK_FLAGS}")
    endif()

    set_target_properties(${LIB_NAME}_shared
                          PROPERTIES LIBRARY_OUTPUT_DIRECTORY
                                     "${OUTPUT_PATH}"
                                     RUNTIME_OUTPUT_DIRECTORY
                                     "${OUTPUT_PATH}"
                                     PDB_OUTPUT_DIRECTORY
                                     "${OUTPUT_PATH}"
                                     LINK_FLAGS
                                     "${ARG_SHARED_LINK_FLAGS}"
                                     OUTPUT_NAME
                                     ${LIB_NAME}
                                     VERSION
                                     "${ICL_FULL_SO_VERSION}"
                                     SOVERSION
                                     "${ICL_SO_VERSION}")

    target_link_libraries(${LIB_NAME}_shared
                          LINK_PUBLIC
                          "$<BUILD_INTERFACE:${ARG_SHARED_LINK_LIBS}>"
                          "$<INSTALL_INTERFACE:${ARG_SHARED_INSTALL_INTERFACE_LIBS}>"
                          LINK_PRIVATE
                          ${ARG_SHARED_PRIVATE_LINK_LIBS})

    if(ICL_RPATH_ORIGIN)
      if(APPLE)
        set(_lib_install_rpath "@loader_path")
      else()
        set(_lib_install_rpath "\$ORIGIN")
      endif()
      set_target_properties(${LIB_NAME}_shared
                            PROPERTIES INSTALL_RPATH ${_lib_install_rpath})
    endif()

    if(APPLE)
      if(ICL_INSTALL_NAME_RPATH)
        set(_lib_install_name "@rpath")
      else()
        set(_lib_install_name "${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_LIBDIR}")
      endif()
      set_target_properties(${LIB_NAME}_shared
                            PROPERTIES BUILD_WITH_INSTALL_RPATH ON INSTALL_NAME_DIR
                                       "${_lib_install_name}")
    endif()

    install(TARGETS ${LIB_NAME}_shared ${INSTALL_IS_OPTIONAL}
            EXPORT ${LIB_NAME}_targets
            RUNTIME DESTINATION ${RUNTIME_INSTALL_DIR}
            LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
            ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
            INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
  endif()

  if(BUILD_STATIC)
    add_library(${LIB_NAME}_static STATIC ${LIB_DEPS})
    if(EXTRA_DEPS)
      add_dependencies(${LIB_NAME}_static ${EXTRA_DEPS})
    endif()

    if(ARG_OUTPUTS)
      list(APPEND ${ARG_OUTPUTS} ${LIB_NAME}_static)
    endif()

    if(LIB_INCLUDES)
      target_include_directories(${LIB_NAME}_static SYSTEM PUBLIC ${ARG_EXTRA_INCLUDES})
    endif()

    if(ARG_PRIVATE_INCLUDES)
      target_include_directories(${LIB_NAME}_static PRIVATE ${ARG_PRIVATE_INCLUDES})
    endif()

    if(MSVC)
      set(LIB_NAME_STATIC ${LIB_NAME}_static)
    else()
      set(LIB_NAME_STATIC ${LIB_NAME})
    endif()

    if(ICL_BUILD_STATIC AND WIN32)
      target_compile_definitions(${LIB_NAME}_static PUBLIC ICL_STATIC)
    endif()

    set_target_properties(${LIB_NAME}_static
                          PROPERTIES LIBRARY_OUTPUT_DIRECTORY "${OUTPUT_PATH}" OUTPUT_NAME
                                     ${LIB_NAME_STATIC})

    if(ARG_STATIC_INSTALL_INTERFACE_LIBS)
      set(INTERFACE_LIBS ${ARG_STATIC_INSTALL_INTERFACE_LIBS})
    else()
      set(INTERFACE_LIBS ${ARG_STATIC_LINK_LIBS})
    endif()

    target_link_libraries(${LIB_NAME}_static LINK_PUBLIC
                          "$<BUILD_INTERFACE:${ARG_STATIC_LINK_LIBS}>"
                          "$<INSTALL_INTERFACE:${INTERFACE_LIBS}>")

    install(TARGETS ${LIB_NAME}_static ${INSTALL_IS_OPTIONAL}
            EXPORT ${LIB_NAME}_targets
            RUNTIME DESTINATION ${RUNTIME_INSTALL_DIR}
            LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
            ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
            INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
  endif()

  # Modify variable in calling scope
  if(ARG_OUTPUTS)
    set(${ARG_OUTPUTS} ${${ARG_OUTPUTS}} PARENT_SCOPE)
  endif()
endfunction()
