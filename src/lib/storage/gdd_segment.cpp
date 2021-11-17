#include "gdd_segment.hpp"

#include "resolve_type.hpp"
#include "storage/vector_compression/base_compressed_vector.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"

namespace opossum {

template <typename T, typename U>
GddSegment<T, U>::GddSegment(pmr_vector<T> block_minima,
                                                       std::optional<pmr_vector<bool>> null_values,
                                                       std::unique_ptr<const BaseCompressedVector> offset_values)
    : AbstractEncodedSegment{data_type_from_type<T>()},
      _block_minima{std::move(block_minima)},
      _null_values{std::move(null_values)},
      _offset_values{std::move(offset_values)},
      _decompressor{_offset_values->create_base_decompressor()} {}

template <typename T, typename U>
const pmr_vector<T>& GddSegment<T, U>::block_minima() const {
  return _block_minima;
}

template <typename T, typename U>
const std::optional<pmr_vector<bool>>& GddSegment<T, U>::null_values() const {
  return _null_values;
}

template <typename T, typename U>
const BaseCompressedVector& GddSegment<T, U>::offset_values() const {
  return *_offset_values;
}

template <typename T, typename U>
AllTypeVariant GddSegment<T, U>::operator[](const ChunkOffset chunk_offset) const {
  PerformanceWarning("operator[] used");
  DebugAssert(chunk_offset < size(), "Passed chunk offset must be valid.");

  const auto typed_value = get_typed_value(chunk_offset);
  if (!typed_value) {
    return NULL_VALUE;
  }
  return *typed_value;
}

template <typename T, typename U>
ChunkOffset GddSegment<T, U>::size() const {
  return static_cast<ChunkOffset>(_offset_values->size());
}

template <typename T, typename U>
std::shared_ptr<AbstractSegment> GddSegment<T, U>::copy_using_allocator(
    const PolymorphicAllocator<size_t>& alloc) const {
  auto new_block_minima = pmr_vector<T>(_block_minima, alloc);
  auto new_offset_values = _offset_values->copy_using_allocator(alloc);

  std::optional<pmr_vector<bool>> null_values;
  if (_null_values) {
    null_values = pmr_vector<bool>(*_null_values, alloc);
  }

  auto copy = std::make_shared<GddSegment>(std::move(new_block_minima), std::move(null_values),
                                                        std::move(new_offset_values));
  copy->access_counter = access_counter;
  return copy;
}

template <typename T, typename U>
size_t GddSegment<T, U>::memory_usage(const MemoryUsageCalculationMode) const {
  // MemoryUsageCalculationMode ignored since full calculation is efficient.
  size_t segment_size =
      sizeof(*this) + sizeof(T) * _block_minima.capacity() + _offset_values->data_size() + sizeof(_null_values);

  if (_null_values) {
    segment_size += _null_values->capacity() / CHAR_BIT;
  }

  return segment_size;
}

template <typename T, typename U>
EncodingType GddSegment<T, U>::encoding_type() const {
  return EncodingType::FrameOfReference;
}

template <typename T, typename U>
std::optional<CompressedVectorType> GddSegment<T, U>::compressed_vector_type() const {
  return _offset_values->type();
}

template class GddSegment<int32_t>;
// int64_t disabled for now, as vector compression cannot handle 64 bit values - also in reference_segment_iterable.hpp
// template class GddSegment<int64_t>;

}  // namespace opossum
