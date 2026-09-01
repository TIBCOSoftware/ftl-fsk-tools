# tibftlfskimportdata — Java tool that replicates Kafka records to FSK brokers.
#
# Requires Java 17+ and Kafka client jars on KAFKA_CLASSPATH.
# Supply at configure time:
#   cmake -DKAFKA_CLASSPATH="/path/kafka-clients.jar:/path/slf4j-api.jar:/path/slf4j-simple.jar" ..

set(KAFKA_CLASSPATH "" CACHE STRING
    "Colon-separated (Unix) or semicolon-separated (Windows) classpath: kafka-clients.jar, slf4j-api.jar, slf4j-simple.jar")

if(NOT KAFKA_CLASSPATH)
    message(WARNING
        "KAFKA_CLASSPATH not set — tibftlfskimportdata will not be compiled.\n"
        "Provide it at cmake configure time, e.g.:\n"
        "  -DKAFKA_CLASSPATH=\"/path/kafka-clients.jar:/path/slf4j-api.jar:/path/slf4j-simple.jar\"")
    return()
endif()

find_package(Java 17 COMPONENTS Development REQUIRED)

set(FSKIMPORT_SRC_DIR    "${CMAKE_CURRENT_SOURCE_DIR}/src/main/java")
set(FSKIMPORT_CLASSES_DIR "${CMAKE_CURRENT_BINARY_DIR}/classes")

file(GLOB_RECURSE FSKIMPORT_JAVA_SOURCES "${FSKIMPORT_SRC_DIR}/*.java")

add_custom_command(
    OUTPUT  "${FSKIMPORT_CLASSES_DIR}/classes.stamp"
    COMMENT "Compiling tibftlfskimportdata"
    COMMAND "${CMAKE_COMMAND}" -E make_directory "${FSKIMPORT_CLASSES_DIR}"
    COMMAND "${Java_JAVAC_EXECUTABLE}"
            -d "${FSKIMPORT_CLASSES_DIR}"
            -cp "${KAFKA_CLASSPATH}"
            ${FSKIMPORT_JAVA_SOURCES}
    COMMAND "${CMAKE_COMMAND}" -E touch "${FSKIMPORT_CLASSES_DIR}/classes.stamp"
    DEPENDS ${FSKIMPORT_JAVA_SOURCES}
)

add_custom_target(tibftlfskimportdata ALL
    DEPENDS "${FSKIMPORT_CLASSES_DIR}/classes.stamp"
)
set_target_properties(tibftlfskimportdata PROPERTIES
    ADDITIONAL_CLEAN_FILES "${FSKIMPORT_CLASSES_DIR}"
)

install(DIRECTORY "${FSKIMPORT_CLASSES_DIR}/"
    DESTINATION share/tibftlfskimportdata/classes
    PATTERN "classes.stamp" EXCLUDE
)
install(FILES "${CMAKE_CURRENT_SOURCE_DIR}/conf/kafka-to-kof.properties"
    DESTINATION share/tibftlfskimportdata/conf
)
install(PROGRAMS "${CMAKE_CURRENT_SOURCE_DIR}/run-apachekafka-to-fsk.sh"
    DESTINATION share/tibftlfskimportdata
)
