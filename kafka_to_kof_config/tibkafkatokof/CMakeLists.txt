file(GLOB_RECURSE tibkafkatokof_sources *)

add_custom_command(OUTPUT ${GOPATH}/bin/tibkafkatokof${CMAKE_EXECUTABLE_SUFFIX}
    COMMENT "Building tibkafkatokof"
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMAND ${CMAKE_COMMAND} -E env
        ${GO_ENV}
        ${GO_CMD} install ${TIB_GO_BUILD_OPTIONS} ${TIB_GO_BUILD_DEBUG_OPTIONS} ./...
    DEPENDS
        ${tibkafkatokof_sources}
)

add_custom_target(tibkafkatokof_build ALL DEPENDS ${GOPATH}/bin/tibkafkatokof${CMAKE_EXECUTABLE_SUFFIX})
set_target_properties(tibkafkatokof_build PROPERTIES ADDITIONAL_CLEAN_FILES ${GOPATH}/bin/tibkafkatokof${CMAKE_EXECUTABLE_SUFFIX})

install(PROGRAMS ${GOPATH}/bin/tibkafkatokof${CMAKE_EXECUTABLE_SUFFIX}
    DESTINATION bin COMPONENT servers)
