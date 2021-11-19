set(
    SHARED_SOURCES
    base_test.cpp
    base_test.hpp
    lib/sql/sqlite_testrunner/sqlite_testrunner.cpp
    lib/sql/sqlite_testrunner/sqlite_testrunner.hpp
    testing_assert.cpp
    testing_assert.hpp
)

set(
    HYRISE_UNIT_TEST_SOURCES
    ${SHARED_SOURCES}
    gtest_case_template.cpp
    gtest_main.cpp
    
    #lib/expression/evaluation/expression_result_test.cpp
    #lib/expression/evaluation/like_matcher_test.cpp
    #lib/expression/expression_evaluator_to_pos_list_test.cpp
    #lib/expression/expression_evaluator_to_values_test.cpp
    #lib/expression/expression_test.cpp
    #lib/expression/expression_utils_test.cpp
    #lib/expression/lqp_subquery_expression_test.cpp
    #lib/expression/pqp_subquery_expression_test.cpp
    
    lib/memory/segments_using_allocators_test.cpp
    
    #lib/operators/aggregate_sort_test.cpp
    #lib/operators/aggregate_test.cpp
    #lib/operators/alias_operator_test.cpp
    #lib/operators/change_meta_table_test.cpp
    #lib/operators/delete_test.cpp
    #lib/operators/difference_test.cpp
    #lib/operators/export_test.cpp
    #lib/operators/get_table_test.cpp
    #lib/operators/import_test.cpp
    #lib/operators/index_scan_test.cpp
    #lib/operators/insert_test.cpp
    #lib/operators/join_hash/join_hash_steps_test.cpp
    #lib/operators/join_hash/join_hash_traits_test.cpp
    #lib/operators/join_hash/join_hash_types_test.cpp
    #lib/operators/join_hash_test.cpp
    #lib/operators/join_index_test.cpp
    #lib/operators/join_nested_loop_test.cpp
    #lib/operators/join_sort_merge_test.cpp
    #lib/operators/join_test_runner.cpp
    #lib/operators/join_verification_test.cpp
    #lib/operators/limit_test.cpp
    
    #lib/operators/table_scan_between_test.cpp
    #lib/operators/table_scan_sorted_segment_search_test.cpp
    #lib/operators/table_scan_string_test.cpp
    #lib/operators/table_scan_test.cpp
    #lib/operators/typed_operator_base_test.hpp
    #lib/operators/union_all_test.cpp
    #lib/operators/union_positions_test.cpp
    #lib/operators/update_test.cpp
    #lib/operators/validate_test.cpp
    #lib/operators/validate_visibility_test.cpp
    
    lib/storage/any_segment_iterable_test.cpp
    lib/storage/chunk_encoder_test.cpp
    lib/storage/chunk_test.cpp
    lib/storage/compressed_vector_test.cpp
    lib/storage/dictionary_segment_test.cpp
    lib/storage/encoded_segment_test.cpp
    lib/storage/encoded_string_segment_test.cpp
    lib/storage/encoding_test.hpp
    lib/storage/fixed_string_dictionary_segment/fixed_string_test.cpp
    lib/storage/fixed_string_dictionary_segment/fixed_string_vector_test.cpp
    lib/storage/fixed_string_dictionary_segment_test.cpp
    lib/storage/index/adaptive_radix_tree/adaptive_radix_tree_index_test.cpp
    lib/storage/index/b_tree/b_tree_index_test.cpp
    lib/storage/index/group_key/composite_group_key_index_test.cpp
    lib/storage/index/group_key/group_key_index_test.cpp
    lib/storage/index/group_key/variable_length_key_base_test.cpp
    lib/storage/index/group_key/variable_length_key_store_test.cpp
    lib/storage/index/group_key/variable_length_key_test.cpp
    lib/storage/index/multi_segment_index_test.cpp
    lib/storage/index/single_segment_index_test.cpp
    lib/storage/iterables_test.cpp
    lib/storage/lz4_segment_test.cpp
    lib/storage/materialize_test.cpp
    lib/storage/pos_lists/entire_chunk_pos_list_test.cpp
    lib/storage/prepared_plan_test.cpp
    lib/storage/reference_segment_test.cpp
    lib/storage/segment_access_counter_test.cpp
    lib/storage/segment_accessor_test.cpp
    lib/storage/segment_iterators_test.cpp
    lib/storage/storage_manager_test.cpp
    lib/storage/table_column_definition_test.cpp
    lib/storage/table_key_constraint_test.cpp
    lib/storage/table_test.cpp
    lib/storage/value_segment_test.cpp
    lib/tasks/chunk_compression_task_test.cpp
    
    testing_assert.cpp
    testing_assert.hpp
)



# Both hyriseTest and hyriseSystemTest link against these
set(
    LIBRARIES
    gtest
    gmock
    sqlite3
    hyriseMvccDeletePlugin  # So that we can test member methods without going through dlsym
)

# This warning does not play well with SCOPED_TRACE
add_compile_options(-Wno-used-but-marked-unused)

# We define TEST_PLUGIN_DIR to always load plugins from the correct directory for testing purposes
add_definitions(-DTEST_PLUGIN_DIR="${CMAKE_BINARY_DIR}/lib/")

# Build special sanitizer version of googletest
include_directories(../../third_party/googletest/googletest/)

include_directories(${CMAKE_CURRENT_SOURCE_DIR})

# Configure hyriseTest
add_executable(hyriseTest ${HYRISE_UNIT_TEST_SOURCES})
add_dependencies(hyriseTest hyriseTestPlugin hyriseMvccDeletePlugin hyriseTestNonInstantiablePlugin)
target_link_libraries(hyriseTest hyrise ${LIBRARIES})

if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
target_compile_options(hyriseTest PUBLIC -Xclang -fno-pch-timestamp)
endif()
if(NOT ${CMAKE_VERSION} VERSION_LESS "3.16.0")
    target_precompile_headers(hyriseTest PRIVATE
        <gtest/gtest.h>
        [["all_parameter_variant.hpp"]]
        [["storage/create_iterable_from_segment.hpp"]]
        [["storage/table.hpp"]]
        [["types.hpp"]]
    )
endif()

# Configure hyriseSystemTest
add_executable(hyriseSystemTest ${SYSTEM_TEST_SOURCES})
target_link_libraries(hyriseSystemTest PUBLIC hyrise hyriseBenchmarkLib ${LIBRARIES})
target_link_libraries_system(hyriseSystemTest pqxx_static)
target_compile_options(hyriseSystemTest PRIVATE -DPQXX_HIDE_EXP_OPTIONAL)
