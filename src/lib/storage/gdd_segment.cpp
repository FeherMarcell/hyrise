#include "gdd_segment.hpp"

#include <vector>
#include <memory>
#include <string>

#include "resolve_type.hpp"
#include "type_comparison.hpp" // for with_comparator

#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"
#include "utils/size_estimation_utils.hpp"

#include "gdd_segment/gdd_lsb/gdd_lsb.hpp"

#include <chrono>

namespace opossum {
using namespace std;
using namespace std::chrono;

template <typename T, typename U>
GddSegmentV1Fixed<T, U>::GddSegmentV1Fixed(const std::shared_ptr<const BasesType>& _bases,
                            const std::shared_ptr<const DeviationsType>& _deviations,
                            const std::shared_ptr<const ReconListType>& _reconstruction_list,
                            const T& segment_min, const T& segment_max, const size_t num_nulls) 
      : BaseGddSegment(data_type_from_type<T>()),
      bases{_bases},
      deviations{_deviations},
      reconstruction_list{_reconstruction_list},
      segment_min{segment_min},
      segment_max{segment_max},
      num_nulls{num_nulls}
{
  // NULL is represented by bases->size() in the reconstruction list. 
  // INVALID_VALUE_ID, which is the highest possible number in ValueID::base_type
  // (2^32 - 1), is needed to represent "value not found" in calls to lower_bound/upper_bound.
  // For a GddSegmentV1Fixed of the max size Chunk::MAX_SIZE, those two values overlap.
  Assert(bases->size() < std::numeric_limits<ValueID::base_type>::max(), "Input segment too big");
}


template <typename T, typename U>
AllTypeVariant GddSegmentV1Fixed<T, U>::operator[](const ChunkOffset chunk_offset) const {
  PerformanceWarning("operator[] used");
  DebugAssert(chunk_offset != INVALID_CHUNK_OFFSET, "Passed chunk offset must be valid.");
  
  const auto typed_value = get_typed_value(chunk_offset);
  if (!typed_value) {
    return NULL_VALUE;
  }
  return *typed_value;
}



template <typename T, typename U>
std::shared_ptr<AbstractSegment> GddSegmentV1Fixed<T, U>::copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const {
  // TODO use allocator

  auto new_bases = std::make_shared<BasesType>(*bases);
  auto new_deviations = std::make_shared<DeviationsType>(*deviations);
  auto new_reconstruction_list = std::make_shared<ReconListType>(*reconstruction_list);
  auto copy = std::make_shared<GddSegmentV1Fixed<T, U>>(
                                                std::move(new_bases), 
                                                std::move(new_deviations), 
                                                std::move(new_reconstruction_list),
                                                segment_min,
                                                segment_max
                                              );
  copy->access_counter = access_counter;
  return copy;
}

// TODO
template <typename T, typename U>
size_t GddSegmentV1Fixed<T, U>::memory_usage(const MemoryUsageCalculationMode mode) const {
  
  return 0U;
  /*
  // Calculate the best compression rate at 
  size_t max_compression_bases = _gdd_profile->at(0), max_compression_dev_bits = 0, best_total_data_bits = 0, total_data_bits=0;
  auto best_compression = gdd_lsb::calculate_compression_rate_percent<T>(max_compression_dev_bits, max_compression_bases, _attribute_vector->size(), best_total_data_bits);
  //for(size_t i=1 ; i<bases_nums.size() ; ++i){
  for(size_t i=1 ; i<=8 ; ++i){

    const auto compression = gdd_lsb::calculate_compression_rate_percent<T>(i, _gdd_profile->at(i), _attribute_vector->size(), total_data_bits);
    if(compression > best_compression){
      max_compression_bases = _gdd_profile->at(i);
      max_compression_dev_bits = i;
      best_total_data_bits = total_data_bits;
    }
  }
  // calculate the size
  return ceil(best_total_data_bits / 8.0);
  */
  /*
  const auto common_elements_size = sizeof(*this) + _attribute_vector->data_size();

  if constexpr (std::is_same_v<T, pmr_string>) {
    return common_elements_size + string_vector_memory_usage(*_dictionary, mode);
  }
  return common_elements_size + _dictionary->size() * sizeof(typename decltype(_dictionary)::element_type::value_type);
  */
}


template <typename T, typename U>
bool GddSegmentV1Fixed<T, U>::isnull(const ChunkOffset& chunk_offset) const {
  if(num_nulls == 0){
    // There are no NULLs
    return false;
  }
  return reconstruction_list->at(chunk_offset) == null_value_id();
}

template <typename T, typename U>
T GddSegmentV1Fixed<T, U>::get(const ChunkOffset& chunk_offset) const {
  DebugAssert(chunk_offset < reconstruction_list->size(), "GddSegmentV1Fixed::get chunkoffset "+std::to_string((size_t)chunk_offset)+" indexes out of reconstruction list, size: "+std::to_string(reconstruction_list->size()));
  DebugAssert(chunk_offset < deviations->size(), "GddSegmentV1Fixed::get chunkoffset larger than deviations!");
  DebugAssert(reconstruction_list->at(chunk_offset) < bases->size(), "GddSegmentV1Fixed::get chunkoffset points to NULL!");

  return gdd_lsb::std_bases::get((size_t)chunk_offset, bases, deviations, reconstruction_list);
}


template <typename T, typename U>
void GddSegmentV1Fixed<T, U>::segment_vs_value_table_scan(
    const PredicateCondition& condition, 
    const AllTypeVariant& query_value, 
    const ChunkID chunk_id, 
    RowIDPosList& matches,
    const std::shared_ptr<const AbstractPosList>& position_filter) const 
{
  const auto typed_query_value = boost::get<T>(query_value);
  DebugAssert(matches.empty(), "Matches are not empty!");

  //const auto t1 = high_resolution_clock::now();
  { // Step 1: early exit based on segment range
    switch(condition) {

      case PredicateCondition::Equals:  
      {
        // If query value is out of range: no matches
        if (typed_query_value < segment_min || typed_query_value > segment_max){
          return;
        }
        break;
      }

      case PredicateCondition::NotEquals: 
      {
        // If query value is out of range: all matches
        if (typed_query_value < segment_min || typed_query_value > segment_max){
          // Add all indexes (chunk offsets)
          _all_to_matches(chunk_id, matches, position_filter);
          return;
        }
        break;
      }

      case PredicateCondition::GreaterThan:
      {
        if (typed_query_value >= segment_max){
          // No match
          return;
        }
        else if (typed_query_value < segment_min){
          // All
          _all_to_matches(chunk_id, matches, position_filter);
          return;
        }
        break;
      }

      case PredicateCondition::GreaterThanEquals:
      {
        if (typed_query_value > segment_max){
          // No match
          return;
        }
        else if (typed_query_value <= segment_min){
          // All
          _all_to_matches(chunk_id, matches, position_filter);
          return;
        }
        break;
      }

      case PredicateCondition::LessThan:
      {
        if (typed_query_value <= segment_min){
          // No match
          return;
        }
        else if (typed_query_value > segment_max){
          // All
          _all_to_matches(chunk_id, matches, position_filter);
          return;
        }
        break;
      }

      case PredicateCondition::LessThanEquals:
      {
        if (typed_query_value < segment_min){
          // No match
          return;
        }
        else if (typed_query_value >= segment_max){
          // All
          _all_to_matches(chunk_id, matches, position_filter);
          return;
        }
        break;
      }
      
      default: break;
    }
  }
  
  //const auto t2 = high_resolution_clock::now();
  // Base index that need to be scanned (if the query value is in an existing base range)
  std::optional<size_t> base_idx_to_scan = std::nullopt;

  { // Step 2: Find base indexes that need to be scanned, add complete bases to matches

    // Calculate the base of the query value
    const auto query_value_base = gdd_lsb::make_base<T, deviation_bits>(typed_query_value);
    
    //const auto t21 = high_resolution_clock::now();

    // We always need the lower bound (e.g. greater or equal), not upper_bound (strictly greater) 
    // regardless of the operator
    const auto lower_it = std::lower_bound(bases->cbegin(), bases->cend(), query_value_base);
    DebugAssert(lower_it != bases->end(), "Base not found! This should have been an early exit!");
    
    //const auto t22 = high_resolution_clock::now();

    // Determine if the query value is inside an existing base range, or an empty region
    const bool is_query_base_present = (*lower_it == query_value_base);
    const size_t query_value_base_idx = std::distance(bases->begin(), lower_it);
    //std::cout << "Number of bases: " << bases->size() << std::endl;
    //std::cout << "Query base present: " << (is_query_base_present ? "yes" : "no") << ", base index: " << query_value_base_idx << std::endl;
    //const auto t23 = high_resolution_clock::now();


    
    // Figure out which bases need to be scanned based on the operator and lower bound
    switch(condition) {
      case PredicateCondition::Equals:
      {
        if(!is_query_base_present){
          // Early exit 2: query hit an empty base region, no matches
          return;
        }
        
        // Scan the base range for equality
        base_idx_to_scan = query_value_base_idx;

        break;
      }

      case PredicateCondition::NotEquals:
      {
        if(!is_query_base_present){
          // Early exit 2: The base of the query value is not present, 
          // therefore no base range is excluded from the result.
          const bool include_nulls = false;
          const bool matches_preallocated = false;
          _all_to_matches(chunk_id, matches, position_filter, include_nulls, matches_preallocated);
          return;
        }

        // Query value is in one of the existing base ranges 
        
        // Scan the base range where the query value hits
        base_idx_to_scan = query_value_base_idx;
        
        // Add all other complete base ranges
        matches.reserve(matches.size() + (position_filter ? position_filter->size() : reconstruction_list->size()));
        if(position_filter){
          // Check only the position filter
          #pragma omp simd
          for(auto i=0U ; i<position_filter->size() ; ++i){
            const auto rowId = (*position_filter)[i];
            DebugAssert(rowId.chunk_id == chunk_id, "Position filter chunkID is different than the current one!");
            if(reconstruction_list->at(rowId.chunk_offset) != query_value_base_idx){
              matches.push_back(RowID{chunk_id, ChunkOffset{i}});
            }
          }
        }
        else {
          // Iterate the full reconstruction list, no position filter is set
          #pragma omp simd
          for(auto i=0U ; i<reconstruction_list->size() ; ++i){
            if(reconstruction_list->at(i) != query_value_base_idx){
              matches.push_back(RowID{chunk_id, ChunkOffset{i}});
            }
          }
        }
        break;
      }

      case PredicateCondition::GreaterThan:
      case PredicateCondition::GreaterThanEquals:
      {

        if(is_query_base_present){
          // Scan the base range
          base_idx_to_scan = query_value_base_idx;

          if(query_value_base_idx == bases->size()-1){
            // Last base is scanned, nothing after it
            break;
          }
        }

        // Add higher base indexes 
        const size_t start_base_idx = is_query_base_present ? query_value_base_idx+1 : query_value_base_idx;
        
        matches.reserve(matches.size() + (position_filter ? position_filter->size() : reconstruction_list->size()));
        if(position_filter){
          // Check only the position filter
          #pragma omp simd
          for(auto i=0U ; i<position_filter->size() ; ++i){
            const auto rowId = (*position_filter)[i];
            DebugAssert(rowId.chunk_id == chunk_id, "Position filter chunkID is different than the current one!");
            if(reconstruction_list->at(rowId.chunk_offset)  >= start_base_idx){
              matches.push_back(RowID{chunk_id, ChunkOffset{i}});
            }
          }
        }
        else {
          // Iterate the full reconstruction list, no position filter is set
          for(auto i=0U ; i<reconstruction_list->size() ; ++i){
            if(reconstruction_list->at(i) >= start_base_idx){
              matches.push_back(RowID{chunk_id, ChunkOffset{i}});
            }
          }
        }
        /*
        #pragma omp simd
        for(auto i=0U ; i<reconstruction_list->size() ; ++i){
          if(reconstruction_list->at(i) >= start_base_idx){
            matches.push_back(RowID{chunk_id, ChunkOffset{i}});
          }
        }
        */
        break;
      }

      case PredicateCondition::LessThan:
      case PredicateCondition::LessThanEquals:
      {
        if(is_query_base_present){
          // Scan the base where the query hits
          base_idx_to_scan = query_value_base_idx;

          if(query_value_base_idx == 0){
            // First base is scanned, nothing before it
            break;
          }
        }

        // Add lower base indexes
        const size_t end_base_idx = (is_query_base_present) ? query_value_base_idx-1 : query_value_base_idx;
        matches.reserve(matches.size() + (position_filter ? position_filter->size() : reconstruction_list->size()));
        if(position_filter){
          // Check only the position filter
          #pragma omp simd
          for(auto i=0U ; i<position_filter->size() ; ++i){
            const auto rowId = (*position_filter)[i];
            DebugAssert(rowId.chunk_id == chunk_id, "Position filter chunkID is different than the current one!");
            if(reconstruction_list->at(rowId.chunk_offset) <= end_base_idx){
              matches.push_back(RowID{chunk_id, ChunkOffset{i}});
            }
          }
        }
        else {
          // Iterate the full reconstruction list, no position filter is set
          #pragma omp simd
          for(auto i=0U ; i<reconstruction_list->size() ; ++i){
            if(reconstruction_list->at(i) <= end_base_idx){
              matches.push_back(RowID{chunk_id, ChunkOffset{i}});
            }
          }
        }
        
        break;
      }

      default: break;
    }

  }

  //const auto t3 = high_resolution_clock::now();

  // Step 3: Scan the base
  if(base_idx_to_scan.has_value()){
    _scan_base(base_idx_to_scan.value(), condition, typed_query_value, chunk_id, matches, position_filter);
  }
  //const auto t4 = high_resolution_clock::now();
  
  /*
  cout << "Total time: " << duration<double, std::milli>(t4-t1).count() << " ms\n"
      << " Early exit: " << duration<double, std::milli>(t2-t1).count() << " ms\n" 
      << " Add complete base ranges: " << duration<double, std::milli>(t3-t2).count() << " ms\n";
  if(base_idx_to_scan.has_value()){
    std::cout << " Scan query base: " << duration<double, std::milli>(t4-t3).count() << " ms";
  }
  std::cout << std::endl;
  */
}



template <typename T, typename U>
void GddSegmentV1Fixed<T, U>::segment_between_table_scan(
    const PredicateCondition& condition, 
    const AllTypeVariant& left_value, 
    const AllTypeVariant& right_value, 
    const ChunkID chunk_id, 
    RowIDPosList& matches,
    const std::shared_ptr<const AbstractPosList>& position_filter) const 
{

  //const auto matches_before = matches.size();
  DebugAssert(matches.size() == 0, "Matches not empty!");

  auto typed_left_value = boost::get<T>(left_value);
  auto typed_right_value = boost::get<T>(right_value);

  //std::cout << "BETWEEN Left value: " << typed_left_value << ", right value: " << typed_right_value << std::endl;
  //std::cout << reconstruction_list->size() << " values in segment, bases num: " << bases->size() << std::endl;
  

  // Make sure left <= right
  if(typed_right_value < typed_left_value){
    
    const auto tmp = typed_left_value;
    typed_left_value = typed_right_value;
    typed_right_value = tmp;
  }


  { // Step 1: early exit based on segment range
    if(typed_right_value < segment_min || typed_left_value > segment_max){
      //std::cout << " early exit: out of range" << std::endl;
      // Query range is completely out of the segment range, no matches
      return;
    }

    if(typed_left_value <= segment_min && typed_right_value >= segment_max){
      // Query range completely includes the segment range, all elements are matches
      //std::cout << " early exit: all values match" << std::endl;
      _all_to_matches(chunk_id, matches, position_filter);
      return;
    }
  }

  {// Step 2: Check if the query bounds are in existing base ranges

    // Left value
    const auto left_value_base = gdd_lsb::make_base<T, deviation_bits>(typed_left_value);
    const auto left_lower_it = std::lower_bound(bases->cbegin(), bases->cend(), left_value_base);
    const bool is_left_base_present = (*left_lower_it == left_value_base);
    // Determine base index that has to be scanned
    const size_t left_base_idx = std::distance(bases->begin(), left_lower_it);
    

    // Right value
    const auto right_value_base = gdd_lsb::make_base<T, deviation_bits>(typed_right_value);
    const auto right_lower_it = std::lower_bound(bases->cbegin(), bases->cend(), right_value_base);
    const bool is_right_base_present = (*right_lower_it == right_value_base);
    // Determine base index that has to be scanned
    const size_t right_base_idx = std::distance(bases->begin(), right_lower_it);

    // Determine which bases need to be scanned and with what operator
    
    // Special case: both query values are in the same base range, this has to be scanned with the BETWEEN op
    if(is_left_base_present && is_right_base_present && left_base_idx == right_base_idx){
      std::cout << "Both values hit base #" << left_base_idx << ", running between scan" << std::endl;
      _scan_base_between(left_base_idx, condition, typed_left_value, typed_right_value, chunk_id, matches, position_filter);
      //return;
    }
    else {
      DebugAssert(left_base_idx != right_base_idx, "Base indexes should not be equal at this point");

      if(is_left_base_present){
        //std::cout << "Left value hits base #" << left_base_idx << std::endl;
        // If the original predicate is exclusive on the left side, we need to scan with the exclusive GreaterThan
        // otherwise with the inclusive GreaterThanEquals
        const auto base_search_predicate = 
          (condition == PredicateCondition::BetweenLowerExclusive || condition == PredicateCondition::BetweenExclusive) ? 
          PredicateCondition::GreaterThan : 
          PredicateCondition::GreaterThanEquals;
        _scan_base(left_base_idx, base_search_predicate, typed_left_value, chunk_id, matches, position_filter);
      }

      if (is_right_base_present){
        //std::cout << "Right value hits base #" << right_base_idx << std::endl;
        // If the original predicate is exclusive on the right side, we need to scan with the exclusive LessThan
        // otherwise with the inclusive LessThanEquals
        const auto base_search_predicate = 
          (condition == PredicateCondition::BetweenUpperExclusive || condition == PredicateCondition::BetweenExclusive) ? 
          PredicateCondition::LessThan : 
          PredicateCondition::LessThanEquals;
        _scan_base(right_base_idx, base_search_predicate, typed_right_value, chunk_id, matches, position_filter);
      }

      // If there are any base ranges between left and right base index, add them
      const size_t start_base_idx = is_left_base_present ? left_base_idx+1 : 0;
      const size_t end_base_idx = is_right_base_present ? right_base_idx : bases->size();
      if(end_base_idx >= start_base_idx){
        //std::cout << "Adding all base indexes from " << start_base_idx << " to " << (end_base_idx-1) << std::endl;
        
        matches.reserve(matches.size() + (position_filter ? position_filter->size() : reconstruction_list->size()));
        for(size_t base_idx = start_base_idx ; base_idx < end_base_idx ; ++base_idx){
          //std::cout << " Adding all values of base #" << base_idx << std::endl;
          //(base_idx, chunk_id, matches, position_filter);

          if(position_filter){
            // Check only the position filter
            #pragma omp simd
            for(auto i=0U ; i<position_filter->size() ; ++i){
              const auto rowId = (*position_filter)[i];
              DebugAssert(rowId.chunk_id == chunk_id, "Position filter chunkID is different than the current one!");
              if(reconstruction_list->at(rowId.chunk_offset) == base_idx){
                matches.push_back(rowId);
              }
            }
          }
          else {
            // Iterate the full reconstruction list, no position filter is set
            #pragma omp simd
            for(auto i=0U ; i<reconstruction_list->size() ; ++i){
              if(reconstruction_list->at(i) == base_idx){
                matches.push_back(RowID{chunk_id, ChunkOffset{i}});
              }
            }
          }
        }
      }
    }
  }

  //const auto matches_after = matches.size();
  //std::cout << "Query finished, " << (matches_after - matches_before) << " new matches found, total matches: " << matches_after << std::endl;
}

template <typename T, typename U>
void GddSegmentV1Fixed<T, U>::_scan_base(
  const size_t& base_index, 
  const PredicateCondition& condition, 
  const T& typed_query_value,
  const ChunkID chunk_id, 
  RowIDPosList& matches,
  const std::shared_ptr<const AbstractPosList>& position_filter) const
{

  // We will use this to skip NULLs in the result set
  const auto null_base_idx = null_value_id();

  with_comparator(condition, [&](auto predicate_comparator) {

    /**
     * Returns whether the given chunk offset satisfies all conditions:
     * - Not a NULL value
     * - The base of the value has the index 'base_idx' in bases vector
     * - The value at chunk offset satisfies the predicate comparator
     * 
     * Note: if a position filter is present, only chunk offsets from the filter should be passed here!
     */ 
    auto if_match = [&](const ChunkOffset& chunk_offset) -> bool 
    {
      const auto current_base_idx = reconstruction_list->at(chunk_offset);
      if(current_base_idx != base_index || current_base_idx == null_base_idx) {
        return false;
      }

      // In this implementation the deviation index just equals to the row index
      const auto dev_idx = chunk_offset;

      // Reconstruct the original value and run comparator
      // (if the base is zero we don't need to reconstruct, just use the deviation directly)
      const T reconstructed_val = gdd_lsb::reconstruct_value<T, deviation_bits>(bases->at(base_index), deviations->at(dev_idx));
      // Evaluate the predicate
      return predicate_comparator(reconstructed_val, typed_query_value);
    };

    // Prepare for adding new matches
    matches.reserve(matches.size() + (position_filter ? position_filter->size() : reconstruction_list->size()));
    // Evaluate the predicate and add qualifying chunk offsets to matches
    // (we need to act differently if a position filter is present)
    if(position_filter){
      
      // Iterate chunk offsets of the position filter only 
      // and add the position filter index to matches for qualifying chunk offsets
      #pragma omp simd
      for(auto i=0U ; i<position_filter->size() ; ++i){
        const auto rowId = (*position_filter)[i];
      
        // Assuming all position filter members refer to this chunk ID!
        DebugAssert(rowId.chunk_id == chunk_id, "Position filter chunkID is different than the current one!");
        
        if(if_match(rowId.chunk_offset)) {
          // If a position filter is given, the chunk offset of matches must be set to the 
          // position filter offset, not the actual chunk offset of the segment!
          matches.push_back(RowID{chunk_id, ChunkOffset{i}});
        }
      }
    }
    else{
      // Iterate the full reconstruction list and simply add qualifying chunk offsets to matches
      #pragma omp simd
      for(auto i=0U ; i<reconstruction_list->size() ; ++i) {
        if(if_match(ChunkOffset{i})){
          matches.push_back(RowID{chunk_id, ChunkOffset{i}});
        }
      }  
    }
  });
}

template <typename T, typename U>
void GddSegmentV1Fixed<T, U>::_scan_base_between(
    const size_t base_index, 
    const PredicateCondition& between_condition, 
    const T& typed_left_value,
    const T& typed_right_value,
    const ChunkID chunk_id, 
    RowIDPosList& matches,
    const std::shared_ptr<const AbstractPosList>& position_filter) const
{
  // Add all values to matches that are between left and right value
  // Note: predicate condition may be any of the 4 Between variants

  // We will use this to skip NULLs in the result set
  const auto null_base_idx = null_value_id();
  with_between_comparator(between_condition, [&](auto between_predicate_comparator) {

    /**
     * Returns whether the given chunk offset satisfies all conditions:
     * - Not a NULL value
     * - The base of the value has the index 'base_idx' in bases vector
     * - The value at chunk offset satisfies the predicate comparator
     * 
     * Note: if a position filter is present, only chunk offsets from the filter should be passed here!
     */ 
    auto if_match = [&](const ChunkOffset& chunk_offset) -> bool 
    {
      const auto current_base_idx = reconstruction_list->at(chunk_offset);
      if(current_base_idx != base_index || current_base_idx == null_base_idx) {
        return false;
      }

      // In this implementation the deviation index just equals to the row index
      const auto dev_idx = chunk_offset;

      // Reconstruct the original value and run comparator
      // (if the base is zero we don't need to reconstruct, just use the deviation directly)
      const T reconstructed_val = gdd_lsb::reconstruct_value<T, deviation_bits>(bases->at(base_index), deviations->at(dev_idx));
      // Evaluate the predicate
      return between_predicate_comparator(reconstructed_val, typed_left_value, typed_right_value);
    };
    
    // Prepare for adding new matches
    matches.reserve(matches.size() + (position_filter ? position_filter->size() : reconstruction_list->size()));
    
    // Evaluate the predicate and add qualifying chunk offsets to matches
    // (we need to act differently if a position filter is present)
    if(position_filter){
      
      // Iterate chunk offsets of the position filter only 
      // and add the position filter index to matches for qualifying chunk offsets
      //auto pos_filter_offset = 0U;
      #pragma omp simd
      for(auto i=0U ; i<position_filter->size() ; ++i){
        const auto rowId = (*position_filter)[i];

        // Assuming all position filter members refer to this chunk ID!
        DebugAssert(rowId.chunk_id == chunk_id, "Position filter chunkID is different than the current one!");
        
        if(if_match(rowId.chunk_offset)) {
          // If a position filter is given, the chunk offset of matches must be set to the 
          // position filter offset, not the actual chunk offset of the segment!
          matches.push_back(RowID{chunk_id, ChunkOffset{i}});
        }
      }
    }
    else{
      // Iterate the full reconstruction list and simply add qualifying chunk offsets to matches
      #pragma omp simd
      for(auto i=0U ; i<reconstruction_list->size() ; ++i) {
        if(if_match(ChunkOffset{i})){
          matches.push_back(RowID{chunk_id, ChunkOffset{i}});
        }
      }  
    }
  });
}

template <typename T, typename U>
void GddSegmentV1Fixed<T, U>::_all_to_matches(
  const ChunkID& chunk_id, 
  RowIDPosList& matches,  
  const std::shared_ptr<const AbstractPosList>& position_filter,
  bool include_nulls, 
  bool are_matches_preallocated) const 
{
  //std::cout << "Adding all ChunkOffsets of the whole segment" << std::endl;
  //const auto t1 = high_resolution_clock::now();
  
  /*
  if(!are_matches_preallocated){
    // Preallocate 'matches' if it has not been already
    //matches.resize(matches.size() + reconstruction_list->size(), RowID{chunk_id, ChunkOffset{0}});
    matches.reserve(matches.size() +  reconstruction_list->size());
  }
  */

  
  
  matches.reserve(matches.size() + (position_filter ? position_filter->size() : reconstruction_list->size()));

  if(include_nulls == true || num_nulls == 0) {
    // ALL values are a match 
    
    if(position_filter){
      // If a position filter is given, matches are indexes to the position filter, not actual chunk offsets!
      // Basically just add all indexes from 0 -> position_filter->size()
      #pragma omp simd
      for(auto i=ChunkOffset{0} ; i<position_filter->size() ; ++i){
        matches.push_back(RowID{chunk_id, i});
      }
    }
    else {
      // Iterate the full reconstruction list, no position filter is set
      #pragma omp simd
      for(auto i=0U ; i<reconstruction_list->size() ; ++i){
        matches.push_back(RowID{chunk_id, ChunkOffset{i}});
      }
    }
  }

  else {  // Filter for NULLs
    
    // We know how many NULLs are there, so we can preallocate the correct size
    //matches.reserve(matches.size() + reconstruction_list->size() - num_nulls);

    // Cache the base index that represents NULLs
    const auto null_base_index = null_value_id();
    if(position_filter){
      // Check only the position filter and add pos filter indexes to matches
      #pragma omp simd
      for(auto i=0U ; i<position_filter->size() ; ++i){
        const auto rowId = (*position_filter)[i];
        
        DebugAssert(rowId.chunk_id == chunk_id, "Position filter chunkID is different than the current one!");
        
        if(reconstruction_list->at(rowId.chunk_offset) != null_base_index){
          matches.push_back(RowID{chunk_id, ChunkOffset{i}});
        }
      }
    }
    else {
      // Iterate the full reconstruction list, no position filter is set
      #pragma omp simd
      for(auto i=0U ; i<reconstruction_list->size() ; ++i){
        if(reconstruction_list->at(i) != null_base_index){
          matches.push_back(RowID{chunk_id, ChunkOffset{i}});
        }
      }
    }
  }
}

template <typename T, typename U>
ChunkOffset GddSegmentV1Fixed<T, U>::size() const {
  // Number of elements = length of the reconstruction list
  return static_cast<ChunkOffset>(reconstruction_list->size());
}


template <typename T, typename U>
ValueID GddSegmentV1Fixed<T, U>::null_value_id() const {
  // Last valid base index + 1
  return ValueID{static_cast<ValueID::base_type>(bases->size())};
}


//EXPLICITLY_INSTANTIATE_DATA_TYPES(GddSegmentV1Fixed);
template class GddSegmentV1Fixed<int64_t>;
template class GddSegmentV1Fixed<int32_t>;

}  // namespace opossum
