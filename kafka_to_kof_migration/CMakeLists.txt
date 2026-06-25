# kafka_to_kof_migration — Java tool that replicates Kafka records to KOF brokers.
#
# Requires Java 11+ and Kafka client jars on KAFKA_CLASSPATH.
# Supply at configure time:
#   cmake -DKAFKA_CLASSPATH="/path/kafka-clients.jar:/path/slf4j-api.jar:/path/slf4j-simple.jar" ..

set(KAFKA_CLASSPATH "" CACHE STRING
    "Colon-separated (Unix) or semicolon-separated (Windows) classpath: kafka-clients.jar, slf4j-api.jar, slf4j-simple.jar")

if(NOT KAFKA_CLASSPATH)
    message(WARNING
        "KAFKA_CLASSPATH not set — kafka_to_kof_migration will not be compiled.\n"
        "Provide it at cmake configure time, e.g.:\n"
        "  -DKAFKA_CLASSPATH=\"/path/kafka-clients.jar:/path/slf4j-api.jar:/path/slf4j-simple.jar\"")
    return()
endif()

find_package(Java 11 COMPONENTS Development REQUIRED)

set(KTK_SRC_DIR    "${CMAKE_CURRENT_SOURCE_DIR}/src/main/java")
set(KTK_CLASSES_DIR "${CMAKE_CURRENT_BINARY_DIR}/classes")

file(GLOB_RECURSE KTK_JAVA_SOURCES "${KTK_SRC_DIR}/*.java")

add_custom_command(
    OUTPUT  "${KTK_CLASSES_DIR}/classes.stamp"
    COMMENT "Compiling kafka_to_kof_migration"
    COMMAND "${CMAKE_COMMAND}" -E make_directory "${KTK_CLASSES_DIR}"
    COMMAND "${Java_JAVAC_EXECUTABLE}"
            -d "${KTK_CLASSES_DIR}"
            -cp "${KAFKA_CLASSPATH}"
            ${KTK_JAVA_SOURCES}
    COMMAND "${CMAKE_COMMAND}" -E touch "${KTK_CLASSES_DIR}/classes.stamp"
    DEPENDS ${KTK_JAVA_SOURCES}
)

add_custom_target(kafka_to_kof_migration ALL
    DEPENDS "${KTK_CLASSES_DIR}/classes.stamp"
)
set_target_properties(kafka_to_kof_migration PROPERTIES
    ADDITIONAL_CLEAN_FILES "${KTK_CLASSES_DIR}"
)

install(DIRECTORY "${KTK_CLASSES_DIR}/"
    DESTINATION share/kafka_to_kof_migration/classes
    PATTERN "classes.stamp" EXCLUDE
)
install(FILES "${CMAKE_CURRENT_SOURCE_DIR}/conf/kafka-to-kof.properties"
    DESTINATION share/kafka_to_kof_migration/conf
)
install(PROGRAMS "${CMAKE_CURRENT_SOURCE_DIR}/run-kafka-to-kof.sh"
    DESTINATION share/kafka_to_kof_migration
)
