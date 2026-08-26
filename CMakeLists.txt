cmake_minimum_required(VERSION 3.15)
project(ftl-fsk-tools LANGUAGES NONE)

# ─── Go tool: tibftlimportconfig ───────────────────────────────────────────────────
# Requires Go 1.24 or later (see tibfsk/importconfig/go.mod).
find_program(GO_EXECUTABLE go
    HINTS /usr/local/go/bin ENV GOROOT_FINAL ENV GOROOT
    REQUIRED
    DOC "Go compiler (1.24+) — override with -DGO_EXECUTABLE=/path/to/go if needed"
)

set(TIBFTLIMPORTCONFIG_SRC "${CMAKE_CURRENT_SOURCE_DIR}/tibfsk/importconfig")
set(TIBFTLIMPORTCONFIG_BIN "${CMAKE_CURRENT_BINARY_DIR}/tibftlimportconfig${CMAKE_EXECUTABLE_SUFFIX}")

file(GLOB_RECURSE TIBFTLIMPORTCONFIG_GO_SOURCES
    "${TIBFTLIMPORTCONFIG_SRC}/*.go"
)

add_custom_command(
    OUTPUT  "${TIBFTLIMPORTCONFIG_BIN}"
    COMMENT "Building tibftlimportconfig"
    WORKING_DIRECTORY "${TIBFTLIMPORTCONFIG_SRC}"
    COMMAND "${CMAKE_COMMAND}" -E env GOTOOLCHAIN=local
            "${GO_EXECUTABLE}" build -o "${TIBFTLIMPORTCONFIG_BIN}" .
    DEPENDS ${TIBFTLIMPORTCONFIG_GO_SOURCES}
            "${TIBFTLIMPORTCONFIG_SRC}/go.mod"
)

add_custom_target(tibftlimportconfig_build ALL
    DEPENDS "${TIBFTLIMPORTCONFIG_BIN}"
)
set_target_properties(tibftlimportconfig_build PROPERTIES
    ADDITIONAL_CLEAN_FILES "${TIBFTLIMPORTCONFIG_BIN}"
)

install(PROGRAMS "${TIBFTLIMPORTCONFIG_BIN}"
    DESTINATION bin
)

# ─── Java tool: tibftlfskimportdata ──────────────────────────────────────────
add_subdirectory(tibfsk/importdata)
