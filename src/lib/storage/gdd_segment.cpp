#include "gdd_segment.hpp"

#include <vector>
#include <memory>
#include <string>

#include "resolve_type.hpp"

#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"
#include "utils/size_estimation_utils.hpp"

#include "gdd_segment/gdd_lsb/gdd_lsb.hpp"


namespace opossum {

template <typename T, typename U>
GddSegmentV1Fixed<T, U>::GddSegmentV1Fixed(const std::shared_ptr<const pmr_vector<T>>& bases,
                            const std::shared_ptr<const compact::vector<unsigned, DEV_BITS>>& deviations,
                            const std::shared_ptr<const compact::vector<size_t>>& base_indexes
      bases{bases},
      deviations{deviations},
      base_indexes{base_indexes} {
  // NULL is represented by bases.size() in the reconstruction list. 
  // INVALID_VALUE_ID, which is the highest possible number in ValueID::base_type
  // (2^32 - 1), is needed to represent "value not found" in calls to lower_bound/upper_bound.
  // For a GddSegmentV1Fixed of the max size Chunk::MAX_SIZE, those two values overlap.

  Assert(bases->size() < std::numeric_limits<ValueID::base_type>::max(), "Input segment too big");
}


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

  auto new_bases = std::make_shared<std::vector<T>>(*new_bases);
  auto new_deviations = std::make_shared<compact::vector<unsigned, DEV_BITS>>(*deviations);
  auto new_base_indexes = std::make_shared<compact::vector<size_t>>(*base_indexes);
  auto copy = std::make_shared<GddSegmentV1Fixed<T, U>>(
                                                std::move(new_bases), 
                                                std::move(new_deviations), 
                                                std::move(new_base_indexes)
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
T GddSegmentV1Fixed<T, U>::get(const ChunkOffset chunk_offset){
  return gdd_lsb::std_bases::get<T>(chunk_offset, *bases, *deviations, base_indexes);
}

template <typename T, typename U>
void GddSegmentV1Fixed<T, U>::segment_vs_value_table_scan(
    const PredicateCondition& condition, 
    const AllTypeVariant& query_value, 
    const ChunkID chunk_id, 
    RowIDPosList& matches,
    const std::shared_ptr<const AbstractPosList>& position_filter) const 
{
  // TODO
  return
}


template <typename T, typename U>
ChunkOffset GddSegmentV1Fixed<T, U>::size() const {
  // Number of elements = length of the reconstruction list
  return static_cast<ChunkOffset>(base_indexes_ptr->size());
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
