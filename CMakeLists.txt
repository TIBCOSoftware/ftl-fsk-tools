cmake_minimum_required(VERSION 3.15)
project(kof-tools LANGUAGES NONE)

# ─── Go tool: tibkafkatokof ───────────────────────────────────────────────────
# Requires Go 1.24 or later (see kafka_to_kof_config/tibkafkatokof/go.mod).
find_program(GO_EXECUTABLE go
    HINTS /usr/local/go/bin ENV GOROOT_FINAL ENV GOROOT
    REQUIRED
    DOC "Go compiler (1.24+) — override with -DGO_EXECUTABLE=/path/to/go if needed"
)

set(TIBKAFKATOKOF_SRC "${CMAKE_CURRENT_SOURCE_DIR}/kafka_to_kof_config/tibkafkatokof")
set(TIBKAFKATOKOF_BIN "${CMAKE_CURRENT_BINARY_DIR}/tibkafkatokof${CMAKE_EXECUTABLE_SUFFIX}")

file(GLOB_RECURSE TIBKAFKATOKOF_GO_SOURCES
    "${TIBKAFKATOKOF_SRC}/*.go"
)

add_custom_command(
    OUTPUT  "${TIBKAFKATOKOF_BIN}"
    COMMENT "Building tibkafkatokof"
    WORKING_DIRECTORY "${TIBKAFKATOKOF_SRC}"
    COMMAND "${CMAKE_COMMAND}" -E env GOTOOLCHAIN=local
            "${GO_EXECUTABLE}" build -o "${TIBKAFKATOKOF_BIN}" .
    DEPENDS ${TIBKAFKATOKOF_GO_SOURCES}
            "${TIBKAFKATOKOF_SRC}/go.mod"
)

add_custom_target(tibkafkatokof_build ALL
    DEPENDS "${TIBKAFKATOKOF_BIN}"
)
set_target_properties(tibkafkatokof_build PROPERTIES
    ADDITIONAL_CLEAN_FILES "${TIBKAFKATOKOF_BIN}"
)

install(PROGRAMS "${TIBKAFKATOKOF_BIN}"
    DESTINATION bin
)

# ─── Java tool: kafka_to_kof_migration ────────────────────────────────────────
add_subdirectory(kafka_to_kof_migration)
