cmake_minimum_required(VERSION 3.10)
project(xgc2_adapter_runtime_client VERSION 0.5.0 LANGUAGES CXX)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

option(XGC2_ADAPTER_RUNTIME_CLIENT_BUILD_TESTING
       "Build xgc2_adapter_runtime_client tests" ON)

find_package(Protobuf REQUIRED)
find_package(PkgConfig REQUIRED)
find_package(Threads REQUIRED)
pkg_check_modules(XGC2PROTOBUF REQUIRED IMPORTED_TARGET xgc2-protobuf=0.5.0)
find_package(xgc2_protobuf REQUIRED CONFIG)
pkg_check_modules(GRPCPP REQUIRED IMPORTED_TARGET grpc++)

find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin)
if(NOT GRPC_CPP_PLUGIN)
  message(FATAL_ERROR "grpc_cpp_plugin was not found")
endif()
if(NOT EXISTS "${XGC2_PROTOBUF_PROTO_ROOT}/xgc/adapter/v1/adapter.proto")
  message(FATAL_ERROR
          "xgc2-protobuf-dev does not provide xgc/adapter/v1/adapter.proto")
endif()

set(XGC2_PROTOCOL_FILES
  xgc/v1/message.proto
  xgc/adapter/v1/adapter.proto
)
set(XGC2_GENERATED_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")
set(XGC2_PROTOCOL_SOURCES)
set(XGC2_PROTOCOL_HEADERS)
set(XGC2_PROTOCOL_INPUTS)
foreach(relative_file IN LISTS XGC2_PROTOCOL_FILES)
  set(input_file "${XGC2_PROTOBUF_PROTO_ROOT}/${relative_file}")
  if(NOT EXISTS "${input_file}")
    message(FATAL_ERROR "xgc2-protobuf-dev is missing ${relative_file}")
  endif()
  string(REGEX REPLACE "\\.proto$" ".pb.cc" generated_source
                       "${relative_file}")
  string(REGEX REPLACE "\\.proto$" ".pb.h" generated_header
                       "${relative_file}")
  list(APPEND XGC2_PROTOCOL_INPUTS "${input_file}")
  list(APPEND XGC2_PROTOCOL_SOURCES
       "${XGC2_GENERATED_DIR}/${generated_source}")
  list(APPEND XGC2_PROTOCOL_HEADERS
       "${XGC2_GENERATED_DIR}/${generated_header}")
endforeach()

add_custom_command(
  OUTPUT ${XGC2_PROTOCOL_SOURCES} ${XGC2_PROTOCOL_HEADERS}
  COMMAND "${CMAKE_COMMAND}" -E make_directory "${XGC2_GENERATED_DIR}"
  COMMAND "${Protobuf_PROTOC_EXECUTABLE}"
          "--proto_path=${XGC2_PROTOBUF_PROTO_ROOT}"
          "--cpp_out=${XGC2_GENERATED_DIR}"
          ${XGC2_PROTOCOL_INPUTS}
  DEPENDS ${XGC2_PROTOCOL_INPUTS}
  COMMENT "Generating Adapter Runtime protobuf C++ bindings"
  VERBATIM
)

set(XGC2_ADAPTER_PROTO
    "${XGC2_PROTOBUF_PROTO_ROOT}/xgc/adapter/v1/adapter.proto")
set(XGC2_ADAPTER_GRPC_SOURCE
    "${XGC2_GENERATED_DIR}/xgc/adapter/v1/adapter.grpc.pb.cc")
set(XGC2_ADAPTER_GRPC_HEADER
    "${XGC2_GENERATED_DIR}/xgc/adapter/v1/adapter.grpc.pb.h")
add_custom_command(
  OUTPUT "${XGC2_ADAPTER_GRPC_SOURCE}" "${XGC2_ADAPTER_GRPC_HEADER}"
  COMMAND "${CMAKE_COMMAND}" -E make_directory "${XGC2_GENERATED_DIR}"
  COMMAND "${Protobuf_PROTOC_EXECUTABLE}"
          "--proto_path=${XGC2_PROTOBUF_PROTO_ROOT}"
          "--grpc_out=${XGC2_GENERATED_DIR}"
          "--plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN}"
          "${XGC2_ADAPTER_PROTO}"
  DEPENDS "${XGC2_ADAPTER_PROTO}"
  COMMENT "Generating AdapterRuntimeLink gRPC C++ binding"
  VERBATIM
)

add_library(xgc2_adapter_runtime_protocol SHARED
  ${XGC2_PROTOCOL_SOURCES}
  "${XGC2_ADAPTER_GRPC_SOURCE}"
)
add_library(xgc2::adapter_runtime_protocol ALIAS
            xgc2_adapter_runtime_protocol)
set_target_properties(xgc2_adapter_runtime_protocol PROPERTIES
  OUTPUT_NAME xgc2_adapter_runtime_protocol
  EXPORT_NAME adapter_runtime_protocol
  VERSION ${PROJECT_VERSION}
  SOVERSION 1
)
target_compile_features(xgc2_adapter_runtime_protocol PUBLIC cxx_std_14)
target_include_directories(xgc2_adapter_runtime_protocol
  PUBLIC
    $<BUILD_INTERFACE:${XGC2_GENERATED_DIR}>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
target_link_libraries(xgc2_adapter_runtime_protocol
  PUBLIC protobuf::libprotobuf
  PRIVATE PkgConfig::GRPCPP
)

add_library(xgc2_adapter_runtime_client SHARED
  src/bootstrap.cpp
  src/client.cpp
  src/control.cpp
  src/digest.cpp
  src/dispatch.cpp
  src/queue.cpp
  src/session.cpp
  src/session_state.cpp
  src/source_dispatch.cpp
  src/spec.cpp
  src/stream.cpp
  src/work.cpp
)
add_library(xgc2::adapter_runtime_client ALIAS xgc2_adapter_runtime_client)
set_target_properties(xgc2_adapter_runtime_client PROPERTIES
  OUTPUT_NAME xgc2_adapter_runtime_client
  EXPORT_NAME adapter_runtime_client
  VERSION ${PROJECT_VERSION}
  SOVERSION 1
)
target_compile_features(xgc2_adapter_runtime_client PUBLIC cxx_std_14)
target_compile_options(xgc2_adapter_runtime_client PRIVATE
  -Wall -Wextra -Wpedantic
)
target_include_directories(xgc2_adapter_runtime_client
  PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
target_link_libraries(xgc2_adapter_runtime_client
  PUBLIC xgc2::adapter_runtime_protocol
  PRIVATE PkgConfig::GRPCPP Threads::Threads
)

install(TARGETS
    xgc2_adapter_runtime_protocol
    xgc2_adapter_runtime_client
  EXPORT xgc2_adapter_runtime_clientTargets
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(DIRECTORY "${XGC2_GENERATED_DIR}/xgc/"
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/xgc"
  FILES_MATCHING PATTERN "*.h"
)
install(DIRECTORY docs/
  DESTINATION
    "${CMAKE_INSTALL_DATADIR}/doc/libxgc2-adapter-runtime-client-dev/docs"
)

configure_package_config_file(
  cmake/xgc2_adapter_runtime_clientConfig.cmake.in
  "${CMAKE_CURRENT_BINARY_DIR}/xgc2_adapter_runtime_clientConfig.cmake"
  INSTALL_DESTINATION
    "${CMAKE_INSTALL_LIBDIR}/cmake/xgc2_adapter_runtime_client"
  PATH_VARS CMAKE_INSTALL_INCLUDEDIR
)
write_basic_package_version_file(
  "${CMAKE_CURRENT_BINARY_DIR}/xgc2_adapter_runtime_clientConfigVersion.cmake"
  VERSION ${PROJECT_VERSION}
  COMPATIBILITY SameMajorVersion
)
install(FILES
  "${CMAKE_CURRENT_BINARY_DIR}/xgc2_adapter_runtime_clientConfig.cmake"
  "${CMAKE_CURRENT_BINARY_DIR}/xgc2_adapter_runtime_clientConfigVersion.cmake"
  DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/xgc2_adapter_runtime_client"
)
install(EXPORT xgc2_adapter_runtime_clientTargets
  NAMESPACE xgc2::
  DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/xgc2_adapter_runtime_client"
)

configure_file(
  pkgconfig/xgc2-adapter-runtime-client.pc.in
  "${CMAKE_CURRENT_BINARY_DIR}/xgc2-adapter-runtime-client.pc"
  @ONLY
)
install(FILES
  "${CMAKE_CURRENT_BINARY_DIR}/xgc2-adapter-runtime-client.pc"
  DESTINATION "${CMAKE_INSTALL_LIBDIR}/pkgconfig"
)

if(XGC2_ADAPTER_RUNTIME_CLIENT_BUILD_TESTING)
  include(CTest)
  if(BUILD_TESTING)
    find_package(GTest REQUIRED)
    add_executable(adapter_runtime_client_test
      test/adapter_runtime_client_cancellation_test.cpp
      test/adapter_runtime_client_handler_test.cpp
      test/adapter_runtime_client_session_test.cpp
      test/adapter_runtime_client_source_test.cpp
    )
    target_compile_features(adapter_runtime_client_test PRIVATE cxx_std_14)
    if(TARGET GTest::gtest)
      set(XGC2_GTEST_TARGET GTest::gtest)
    else()
      set(XGC2_GTEST_TARGET GTest::GTest)
    endif()
    if(TARGET GTest::gtest_main)
      set(XGC2_GTEST_MAIN_TARGET GTest::gtest_main)
    else()
      set(XGC2_GTEST_MAIN_TARGET GTest::Main)
    endif()
    target_link_libraries(adapter_runtime_client_test
      PRIVATE
        xgc2::adapter_runtime_client
        PkgConfig::GRPCPP
        ${XGC2_GTEST_TARGET}
        ${XGC2_GTEST_MAIN_TARGET}
        Threads::Threads
    )
    include(GoogleTest)
    gtest_discover_tests(adapter_runtime_client_test DISCOVERY_TIMEOUT 10)
  endif()
endif()
