get_target_property(SOMA_INCLUDE_DIRS soma-client INCLUDE_DIRECTORIES)

set(CALIPER_SOMA_SOURCES
  SomaForwarder.cpp)

add_library(caliper-soma OBJECT ${CALIPER_SOMA_SOURCES})
target_link_libraries(caliper-soma PRIVATE soma-client)
target_include_directories(caliper-soma PRIVATE ${MPI_CXX_INCLUDE_PATH})

add_service_objlib("caliper-soma")
add_caliper_service("soma CALIPER_HAVE_SOMA")
