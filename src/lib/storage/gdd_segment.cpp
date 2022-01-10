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
GddSegmentV1Fixed<T, U>::GddSegmentV1Fixed(const std::shared_ptr<const std::vector<T>>& _bases,
                            const std::shared_ptr<const DeviationsCV>& _deviations,
                            const std::shared_ptr<const compact::vector<size_t>>& _reconstruction_list,
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


// TODO this returns teh base, not the actual value!!
template <typename T, typename U>
AllTypeVariant GddSegmentV1Fixed<T, U>::operator[](const ChunkOffset chunk_offset) const {
  PerformanceWarning("operator[] used");
  DebugAssert(chunk_offset != INVALID_CHUNK_OFFSET, "Passed chunk offset must be valid.");
  access_counter[SegmentAccessCounter::AccessType::Dictionary] += 1;
  const auto typed_value = get_typed_value(chunk_offset);
  if (!typed_value) {
    return NULL_VALUE;
  }
  return *typed_value;
}



template <typename T, typename U>
std::shared_ptr<AbstractSegment> GddSegmentV1Fixed<T, U>::copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const {
  // TODO use allocator

  auto new_bases = std::make_shared<std::vector<T>>(*bases);
  auto new_deviations = std::make_shared<DeviationsCV>(*deviations);
  auto new_reconstruction_list = std::make_shared<compact::vector<size_t>>(*reconstruction_list);
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
T GddSegmentV1Fixed<T, U>::get(const ChunkOffset& chunk_offset) const {
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
          _all_to_matches(chunk_id, matches);
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
          _all_to_matches(chunk_id, matches);
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
          _all_to_matches(chunk_id, matches);
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
          _all_to_matches(chunk_id, matches);
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
          _all_to_matches(chunk_id, matches);
          return;
        }
        break;
      }
      
      default: break;
    }
  }
  
  //const auto t2 = high_resolution_clock::now();
  // Base indexes that need to be scanned
  vector<size_t> base_indexes_to_scan;

  // Aggressively preallocate matches for the maximum size
  matches.reserve(matches.size() + reconstruction_list->size());
  
  { // Step 2: Find base indexes that need to be scanned, add complete bases to matches

    // Calculate the base of the query value
    const auto query_value_base = gdd_lsb::make_base<T, deviation_bits>(typed_query_value);
    
    //const auto t21 = high_resolution_clock::now();

    // We always need the lower bound (e.g. greater or equal), not upper_bound (strictly greater) 
    // regardless of the operator
    const auto lower_it = std::lower_bound(bases->cbegin(), bases->cend(), query_value_base);
    //Assert(lower_it != bases->end(), "No lower bound found! This should have been an early exit!");
    
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
        
        // The found base is the only one that needs to be checked
        base_indexes_to_scan.push_back(query_value_base_idx);
        // No matches possible in other base ranges
        break;
      }

      case PredicateCondition::NotEquals:
      {
        if(!is_query_base_present){
          // Early exit 2: The base of the query value is not present, 
          // therefore no base range is excluded from the result.
          const bool include_nulls = false;
          const bool matches_preallocated = true;
          _all_to_matches(chunk_id, matches, include_nulls, matches_preallocated);
          return;
        }

        // Query value is in one of the existing base ranges 
        
        // This base have to be scanned, but all other values are matches
        base_indexes_to_scan.push_back(query_value_base_idx);
        
        // Add all other complete base ranges
        #pragma omp simd
        for(auto i=0U ; i<reconstruction_list->size() ; ++i){
          if(reconstruction_list->at(i) != query_value_base_idx){
            matches.push_back(RowID{chunk_id, ChunkOffset{(unsigned)i}});
          }
        }
        break;
      }

      case PredicateCondition::GreaterThan:
      case PredicateCondition::GreaterThanEquals:
      {

        /**
         * TODO if the operator is GreaterThan and the query value is 
         * the FIRST value of the lower bound base range, we can skip the lower bound
         * because nothing from there will qualify, and only focus on the rest of the bases.
         * For example: 
         *  - Segment values: 0,1,2,3,..., 200
         *  - 6-bit deviations (base range: 64), 
         *  - Query: col > 127
         * '127' is in base range of base #1, lower_bound points to base #1.
         * However, since 127 is the last value of base range #1 (64-127), we
         * know without scanning that no values from this range will be >127.
         */


        if(is_query_base_present){
          // Scan the base where the query hits
          base_indexes_to_scan.push_back(query_value_base_idx);

          if(query_value_base_idx == bases->size()-1){
            // Last base is scanned, nothing after it
            break;
          }
        }

        // Add higher base indexes 
        const size_t start_base_idx = is_query_base_present ? query_value_base_idx+1 : query_value_base_idx;
        #pragma omp simd
        for(auto i=0U ; i<reconstruction_list->size() ; ++i){
          if(reconstruction_list->at(i) >= start_base_idx){
            matches.push_back(RowID{chunk_id, ChunkOffset{(unsigned)i}});
          }
        }
        break;
      }

      case PredicateCondition::LessThan:
      case PredicateCondition::LessThanEquals:
      {
        
        // Add all base indexes from the beginning up to lower_idx.
        if(is_query_base_present){
          // Scan the base where the query hits
          base_indexes_to_scan.push_back(query_value_base_idx);

          if(query_value_base_idx == 0){
            // First base is scanned, nothing before it
            break;
          }
        }

        // Add lower base indexes
        const size_t end_base_idx = (is_query_base_present) ? query_value_base_idx-1 : query_value_base_idx;
        #pragma omp simd
        for(auto i=0U ; i<reconstruction_list->size() ; ++i){
          if(reconstruction_list->at(i) <= end_base_idx){
            matches.push_back(RowID{chunk_id, ChunkOffset{(unsigned)i}});
          }
        }
        break;
      }

      default: break;
    }

  }

  //const auto t3 = high_resolution_clock::now();

  // Step 3: Scan the base or bases (in case of BETWEEN operator)
  _scan_bases(base_indexes_to_scan, condition, typed_query_value, chunk_id, matches);
  //const auto t4 = high_resolution_clock::now();
  
  /*
  cout << "Total time: " << duration<double, std::milli>(t4-t1).count() << " ms\n"
      << " Early exit: " << duration<double, std::milli>(t2-t1).count() << " ms\n" 
      << " Find base indexes & add complete bases: " << duration<double, std::milli>(t3-t2).count() << " ms\n" 
      << " Scan bases: " << duration<double, std::milli>(t4-t3).count() << " ms" << endl;
  */
  return;
}

template <typename T, typename U>
void GddSegmentV1Fixed<T, U>::_scan_bases(
    const std::vector<size_t>& base_indexes, 
    const PredicateCondition& condition, 
    const T& typed_query_value,
    const ChunkID chunk_id, 
    RowIDPosList& matches) const
{
  /*
  std::cout << "Scanning " << base_indexes.size() << "/" << bases->size() << " bases: "; 
  for(const auto& i : base_indexes){ std::cout << "#" << i << " "; }
  std::cout << std::endl;
  */
  //const auto t1 = high_resolution_clock::now();

  // We will use this to skip NULLs in the result set
  const auto null_base_idx = null_value_id();

  
  /* v1, slow
  with_comparator(condition, [&](auto predicate_comparator) {

    for(auto i=0U ; i<reconstruction_list->size() ; ++i) {
      const auto current_base_idx = reconstruction_list->at(i);
      
      // Check if the current base index is one of the bases we need to scan
      // (base_indexes is sorted, we can search with lower_bound)
      bool scan_current_value;
      if(base_indexes.size() == 1){
        scan_current_value = (base_indexes.front() == current_base_idx);
      }
      else{
        const auto base_index_lower = std::lower_bound(base_indexes.cbegin(), base_indexes.cend(), current_base_idx);
        scan_current_value = *base_index_lower == current_base_idx;
      }

      if(scan_current_value) {
        // Yes, scan this value
        
        // 'i' is a deviation index , reconstruction_list[i] is the base index
        const auto dev_idx = i;
        const auto base_idx = reconstruction_list->at(i);

        if(base_idx == null_base_idx){
          // Skip NULLs
          continue;
        }

        // Reconstruct the original value and run comparator
        // If the base is zero we don't need to reconstruct, just use the deviation directly
        const T reconstructed_val = (bases->at(base_idx) == 0) ? 
                                      deviations->at(dev_idx) : 
                                      gdd_lsb::reconstruct_value<T, deviation_bits>(bases->at(base_idx), deviations->at(dev_idx));
        // Evaluate the predicate
        if(predicate_comparator(reconstructed_val, typed_query_value)){
          matches.push_back(RowID{chunk_id, ChunkOffset{(unsigned) dev_idx}});
        }
        
      }
    }
  });
  */

  with_comparator(condition, [&](auto predicate_comparator) {

    for(const auto& current_base_idx : base_indexes){
      // Find reconstruction_list entries for the current base index
      #pragma omp simd
      for(auto i=0U ; i<reconstruction_list->size() ; ++i) {
        if(reconstruction_list->at(i) != current_base_idx || reconstruction_list->at(i) == null_base_idx){
          continue;
        }
        // scan base
        const auto dev_idx = i;

        // Reconstruct the original value and run comparator
        // If the base is zero we don't need to reconstruct, just use the deviation directly
        const T reconstructed_val = (bases->at(current_base_idx) == 0) ? 
                                      deviations->at(dev_idx) : 
                                      gdd_lsb::reconstruct_value<T, deviation_bits>(bases->at(current_base_idx), deviations->at(dev_idx));
        // Evaluate the predicate
        if(predicate_comparator(reconstructed_val, typed_query_value)){
          matches.push_back(RowID{chunk_id, ChunkOffset{(unsigned) dev_idx}});
        }
      }
    }
  });
  
  /**
   *  SCAN A SINGLE BASE RANGE (less efficient than collecting deviations from all base ranges at once)
    vector<size_t> deviation_indexes;

    { // Find all deviation indexes that use this base (TODO null values!)

      for(auto i=0U ; i<reconstruction_list->size() ; ++i){
        if(reconstruction_list->at(i) == base_idx){
          deviation_indexes.push_back(i);
        }
      }
      Assert(!deviation_indexes.empty(), "No deviations for base #"+std::to_string(base_idx));
      //std::cout << deviation_indexes.size() << " deviations use base #" << base_idx << endl;
    }

    { // Reconstruct original values and evaluate predicate to find matches in this segment (this is independent of the predicate!)
      const auto base = bases->at(base_idx);

      // Reserve enough space in matches so we can push_back all indexes without allocating memory in a loop
      matches.reserve(matches.size() + deviation_indexes.size());
      // Convert the condition (enum) to a bool Functor
      with_comparator(condition, [&](auto predicate_comparator) {

        for(const auto& dev_idx : deviation_indexes){
          // If the base is zero we don't need to reconstruct, just use the deviation directly
          const T reconstructed_val = (base == 0) ? 
                                        deviations->at(dev_idx) : 
                                        gdd_lsb::reconstruct_value<T, deviation_bits>(bases->at(base_idx), deviations->at(dev_idx));
          // Evaluate the predicate
          if(predicate_comparator(reconstructed_val, typed_query_value)){
            matches.push_back(RowID{chunk_id, ChunkOffset{(unsigned) dev_idx}});
          }
        } 
      });
    }
  */
  //const auto t2 = high_resolution_clock::now();
  //cout << "Scanning " << base_indexes.size() << " bases took " << duration<double, std::milli>(t2-t1).count() << " ms" << endl;
}

template <typename T, typename U>
void GddSegmentV1Fixed<T, U>::_all_to_matches(const ChunkID& chunk_id, RowIDPosList& matches, bool include_nulls, bool are_matches_preallocated) const {
  //std::cout << "Adding all ChunkOffsets of the whole segment" << std::endl;
  //const auto t1 = high_resolution_clock::now();
  
  const auto match_insert_idx = matches.size();
  if(!are_matches_preallocated){
    // Preallocate 'matches' if it has not been already
    //matches.reserve(matches.size() + reconstruction_list->size());
    matches.resize(matches.size() + reconstruction_list->size(), RowID{chunk_id, ChunkOffset{0}});
  }

  if(include_nulls == true || num_nulls == 0) {
    // ALL values are a match 
    #pragma omp simd
    for(auto i=0U ; i<reconstruction_list->size() ; ++i){
      matches[match_insert_idx+i].chunk_offset = i;
      //matches.push_back(RowID{chunk_id, ChunkOffset{(unsigned)i}});
    }
  }

  else {  // Filter for NULLs
    

    // We know how many NULLs are there, so we can preallocate the correct size
    //matches.reserve(matches.size() + reconstruction_list->size() - num_nulls);

    // Cache the base index that represents NULLs
    const auto null_base_index = null_value_id();
    #pragma omp simd
    // Iterate the reconstruction list and only add a match if it's not a NULL
    for(auto i=0U ; i<reconstruction_list->size() ; ++i){
      if(reconstruction_list->at(i) != null_base_index){
        matches.push_back(RowID{chunk_id, ChunkOffset{(unsigned)i}});
      }
    }
  }
  /*
  const auto t2 = high_resolution_clock::now();
  std::cout << "Adding all ChunkOffsets to matches took " << duration<double, std::milli>(t2-t1).count() << " ms" ;
  if(!are_matches_preallocated){ cout << " (including preallocating matches)"; }
  std::cout << std::endl;
  */
}

template <typename T, typename U>
void GddSegmentV1Fixed<T, U>::_base_idx_to_matches(const size_t base_idx, const ChunkID& chunk_id, RowIDPosList& matches) const {
  return;
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
