#include "gdd_segment.hpp"

#include <memory>
#include <string>

#include "resolve_type.hpp"
#include "storage/vector_compression/base_compressed_vector.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"
#include "utils/size_estimation_utils.hpp"
#include "gdd_segment/gdd_lsb.hpp"


namespace opossum {

template <typename T, typename U>
GddSegment<T, U>::GddSegment(const std::shared_ptr<const pmr_vector<T>>& dictionary,
                              const std::shared_ptr<const BaseCompressedVector>& attribute_vector,
                              const std::shared_ptr<const pmr_vector<uint32_t>>& gdd_profile)
    : BaseGddSegment(data_type_from_type<T>()),
      _dictionary{dictionary},
      _attribute_vector{attribute_vector},
      _decompressor{_attribute_vector->create_base_decompressor()}, 
      _gdd_profile{gdd_profile} {
  // NULL is represented by _dictionary.size(). INVALID_VALUE_ID, which is the highest possible number in
  // ValueID::base_type (2^32 - 1), is needed to represent "value not found" in calls to lower_bound/upper_bound.
  // For a GddSegment of the max size Chunk::MAX_SIZE, those two values overlap.

  Assert(_dictionary->size() < std::numeric_limits<ValueID::base_type>::max(), "Input segment too big");
}

template <typename T, typename U>
AllTypeVariant GddSegment<T, U>::operator[](const ChunkOffset chunk_offset) const {
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
std::shared_ptr<const pmr_vector<T>> GddSegment<T, U>::dictionary() const {
  // We have no idea how the dictionary will be used, so we do not increment the access counters here
  return _dictionary;
}

template <typename T, typename U>
std::shared_ptr<const BaseCompressedVector> GddSegment<T, U>::attribute_vector() const {
  return _attribute_vector;
}

template <typename T, typename U>
std::shared_ptr<const pmr_vector<uint32_t>> GddSegment<T, U>::gdd_profile() const {
  return _gdd_profile;
}

template <typename T, typename U>
ValueID GddSegment<T, U>::null_value_id() const {
  return ValueID{static_cast<ValueID::base_type>(_dictionary->size())};
}

template <typename T, typename U>
ChunkOffset GddSegment<T, U>::size() const {
  return static_cast<ChunkOffset>(_attribute_vector->size());
}

template <typename T, typename U>
std::shared_ptr<AbstractSegment> GddSegment<T, U>::copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const {
  auto new_attribute_vector = _attribute_vector->copy_using_allocator(alloc);
  auto new_dictionary = std::make_shared<pmr_vector<T>>(*_dictionary, alloc);
  auto new_gdd_profile = std::make_shared<pmr_vector<uint32_t>>(*_gdd_profile, alloc);
  auto copy = std::make_shared<GddSegment<T, U>>(
                                                std::move(new_dictionary), 
                                                std::move(new_attribute_vector), 
                                                std::move(new_gdd_profile)
                                              );
  copy->access_counter = access_counter;
  return copy;
}

template <typename T, typename U>
size_t GddSegment<T, U>::memory_usage(const MemoryUsageCalculationMode mode) const {
  
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
  /*
  const auto common_elements_size = sizeof(*this) + _attribute_vector->data_size();

  if constexpr (std::is_same_v<T, pmr_string>) {
    return common_elements_size + string_vector_memory_usage(*_dictionary, mode);
  }
  return common_elements_size + _dictionary->size() * sizeof(typename decltype(_dictionary)::element_type::value_type);
  */
}

template <typename T, typename U>
std::optional<CompressedVectorType> GddSegment<T, U>::compressed_vector_type() const {
  return _attribute_vector->type();
}

template <typename T, typename U>
EncodingType GddSegment<T, U>::encoding_type() const {
  return EncodingType::GDD;
}

template <typename T, typename U>
ValueID GddSegment<T, U>::lower_bound(const AllTypeVariant& value) const {
  DebugAssert(!variant_is_null(value), "Null value passed.");
  access_counter[SegmentAccessCounter::AccessType::Dictionary] +=
      static_cast<uint64_t>(std::ceil(std::log2(_dictionary->size())));
  const auto typed_value = boost::get<T>(value);
  // Find lower bound in the dictionary 
  auto it = std::lower_bound(_dictionary->cbegin(), _dictionary->cend(), typed_value);
  if (it == _dictionary->cend()) return INVALID_VALUE_ID;
  return ValueID{static_cast<ValueID::base_type>(std::distance(_dictionary->cbegin(), it))};
}

template <typename T, typename U>
ValueID GddSegment<T, U>::upper_bound(const AllTypeVariant& value) const {
  DebugAssert(!variant_is_null(value), "Null value passed.");
  access_counter[SegmentAccessCounter::AccessType::Dictionary] +=
      static_cast<uint64_t>(std::ceil(std::log2(_dictionary->size())));
  const auto typed_value = boost::get<T>(value);

  auto it = std::upper_bound(_dictionary->cbegin(), _dictionary->cend(), typed_value);
  if (it == _dictionary->cend()) return INVALID_VALUE_ID;
  return ValueID{static_cast<ValueID::base_type>(std::distance(_dictionary->cbegin(), it))};
}

template <typename T, typename U>
AllTypeVariant GddSegment<T, U>::value_of_value_id(const ValueID value_id) const {
  DebugAssert(value_id < _dictionary->size(), "ValueID out of bounds");
  access_counter[SegmentAccessCounter::AccessType::Dictionary] += 1;
  return (*_dictionary)[value_id];
}

template <typename T, typename U>
ValueID::base_type GddSegment<T, U>::unique_values_count() const {
  return static_cast<ValueID::base_type>(_dictionary->size());
}


//EXPLICITLY_INSTANTIATE_DATA_TYPES(GddSegment);
template class GddSegment<int64_t>;
template class GddSegment<int32_t>;

}  // namespace opossum
