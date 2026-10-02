cmake_minimum_required(VERSION 3.24)
project(riemann_go LANGUAGES NONE)
if(CMAKE_SOURCE_DIR STREQUAL CMAKE_BINARY_DIR)
  message(FATAL_ERROR "Use an out-of-source build: cmake -S . -B build")
endif()
find_package(Python3 3.11 REQUIRED COMPONENTS Interpreter)
set(RG_MODEL "" CACHE STRING "Exact generation model identifier")
set(RG_AGENT_COMMAND_FILE "" CACHE FILEPATH "JSON argv array for the coding agent; {model} is substituted")
set(RG_GENERATION_ID "default" CACHE STRING "Change to request a fresh generation with unchanged inputs")
if(RG_MODEL STREQUAL "" OR NOT EXISTS "${RG_AGENT_COMMAND_FILE}")
  message(FATAL_ERROR "Set RG_MODEL and RG_AGENT_COMMAND_FILE. See GENERATE.md.")
endif()
get_filename_component(RG_AGENT_COMMAND_FILE "${RG_AGENT_COMMAND_FILE}" ABSOLUTE)
configure_file(cmake/generation-settings.txt.in generation-settings.txt @ONLY)
set(artifact "${CMAKE_BINARY_DIR}/service")
set(manifest "${CMAKE_BINARY_DIR}/build-manifest.json")
set(spec "${CMAKE_BINARY_DIR}/SPEC.md")
add_custom_command(
  OUTPUT "${artifact}" "${manifest}" "${spec}"
  COMMAND "${Python3_EXECUTABLE}" "${CMAKE_SOURCE_DIR}/bin/generate"
    --model "${RG_MODEL}" --command-file "${RG_AGENT_COMMAND_FILE}"
    --build-dir "${CMAKE_BINARY_DIR}"
  DEPENDS "${CMAKE_SOURCE_DIR}/SPEC.md" "${CMAKE_SOURCE_DIR}/SEMANTICS.md"
    "${CMAKE_SOURCE_DIR}/SCALE.md" "${CMAKE_SOURCE_DIR}/INVARIANTS.md"
    "${CMAKE_SOURCE_DIR}/knowledge/INDEX.md" "${CMAKE_SOURCE_DIR}/bin/prompt.md"
    "${CMAKE_SOURCE_DIR}/bin/generate"
    "${RG_AGENT_COMMAND_FILE}" "${CMAKE_BINARY_DIR}/generation-settings.txt"
  COMMENT "Generating and building riemannd from its specification"
  USES_TERMINAL VERBATIM)
add_custom_target(generate-service ALL DEPENDS "${artifact}" "${manifest}" "${spec}")
include(CTest)
if(BUILD_TESTING)
  add_test(NAME spec-conformance COMMAND "${Python3_EXECUTABLE}"
    "${CMAKE_SOURCE_DIR}/bin/test-build" --build-dir "${CMAKE_BINARY_DIR}")
  # The development corpus settles for about 65 s in total plus stimulus and
  # process start; 900 s leaves room for a slow candidate without masking a hang.
  set_tests_properties(spec-conformance PROPERTIES TIMEOUT 900)
endif()
add_custom_target(check-harness COMMAND "${Python3_EXECUTABLE}"
  "${CMAKE_SOURCE_DIR}/bin/check" WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}"
  USES_TERMINAL VERBATIM)
