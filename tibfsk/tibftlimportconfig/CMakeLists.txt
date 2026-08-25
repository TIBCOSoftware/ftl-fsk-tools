file(GLOB_RECURSE tibftlimportconfig_sources *)

add_custom_command(OUTPUT ${GOPATH}/bin/tibftlimportconfig${CMAKE_EXECUTABLE_SUFFIX}
    COMMENT "Building tibftlimportconfig"
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMAND ${CMAKE_COMMAND} -E env
        ${GO_ENV}
        ${GO_CMD} install ${TIB_GO_BUILD_OPTIONS} ${TIB_GO_BUILD_DEBUG_OPTIONS} ./...
    DEPENDS
        ${tibftlimportconfig_sources}
)

add_custom_target(tibftlimportconfig_build ALL DEPENDS ${GOPATH}/bin/tibftlimportconfig${CMAKE_EXECUTABLE_SUFFIX})
set_target_properties(tibftlimportconfig_build PROPERTIES ADDITIONAL_CLEAN_FILES ${GOPATH}/bin/tibftlimportconfig${CMAKE_EXECUTABLE_SUFFIX})

install(PROGRAMS ${GOPATH}/bin/tibftlimportconfig${CMAKE_EXECUTABLE_SUFFIX}
    DESTINATION bin COMPONENT servers)
