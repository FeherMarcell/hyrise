#pragma once

#include <array>
#include <memory>
#include <type_traits>

#include <boost/hana/contains.hpp>
#include <boost/hana/tuple.hpp>
#include <boost/hana/type.hpp>

#include "abstract_encoded_segment.hpp"
#include "types.hpp"

namespace opossum {

/**
 * @brief Segment implementing Generalized Deduplication encoding
 * 
 * GDDTODO: write simple overview of GDD, like the 2 paragraphs below
 * 
 * std::enable_if_t must be used here and cannot be replaced by a
 * static_assert in order to prevent instantiation of
 * GddSegment<T> with T other than the supported data types (uint32_t, uint64_t). 
 * Otherwise, the compiler might instantiate GddSegment with other
 * types even if they are never actually needed.
 * 
 */
template <typename T, typename=std::enable_if_t<encoding_supports_data_type(enum_c<EncodingType, EncodingType::GDD>, hana::type_c<T>)>>
class GddSegment : public AbstractEncodedSegment {
 public:
  explicit GddSegment(const std::shared_ptr<const pmr_vector<T>>& values,
                      const std::shared_ptr<const pmr_vector<bool>>& null_values);

  std::shared_ptr<const pmr_vector<T>> values() const;
  std::shared_ptr<const pmr_vector<bool>> null_values() const;

  /**
   * @defgroup AbstractSegment interface
   * @{
   */

  AllTypeVariant operator[](const ChunkOffset chunk_offset) const final;

  std::optional<T> get_typed_value(const ChunkOffset chunk_offset) const {
    // performance critical - not in cpp to help with inlining
    //const auto end_position_it = std::lower_bound(_end_positions->cbegin(), _end_positions->cend(), chunk_offset);
    //const auto index = std::distance(_end_positions->cbegin(), end_position_it);

    const auto is_null = (*_null_values)[chunk_offset];
    if (is_null) {
      return std::nullopt;
    }
    // TODO decompress and return value
    return (*_values)[chunk_offset];
  }

  ChunkOffset size() const final;

  std::shared_ptr<AbstractSegment> copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const final;

  size_t memory_usage(const MemoryUsageCalculationMode mode) const final;

  /**@}*/

  /**
   * @defgroup AbstractEncodedSegment interface
   * @{
   */

  EncodingType encoding_type() const final;
  std::optional<CompressedVectorType> compressed_vector_type() const final;

  /**@}*/

 protected:
  const std::shared_ptr<const pmr_vector<T>> _values;
  const std::shared_ptr<const pmr_vector<bool>> _null_values;
};

//EXPLICITLY_DECLARE_DATA_TYPES(GddSegment);
extern template class GddSegment<int32_t>;
extern template class GddSegment<int64_t>;

}  // namespace opossum
