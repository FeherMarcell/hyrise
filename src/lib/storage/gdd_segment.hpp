#pragma once

#include <memory>
#include <string>
#include <vector>

#include "base_gdd_segment.hpp"
#include "storage/vector_compression/base_compressed_vector.hpp"
#include "types.hpp"

#include "compact_vector.hpp"

namespace opossum {

class BaseCompressedVector;

/**
 * Segment implementing GDD compression in the following way:
 * 
 *  - Fixed 8-bit deviations
 *  - No deviation lookup hashmap
 *  - Deviations are not deduplicated
 */
template <typename T, typename=std::enable_if_t<encoding_supports_data_type(enum_c<EncodingType, EncodingType::GDD>, hana::type_c<T>)>>
class GddSegmentV1Fixed : public BaseGddSegment {
 public:

  // Constant 8-bit GDD deviations 
  static const auto deviation_bits = 8U;

  using DeviationsCV = compact::vector<unsigned, deviation_bits>;
  
  explicit GddSegmentV1Fixed(const std::shared_ptr<const std::vector<T>>& bases,
                            const std::shared_ptr<const DeviationsCV>& deviations,
                            const std::shared_ptr<const compact::vector<size_t>>& reconstruction_list,
                            const T& segment_min=0, const T& segment_max=0, const size_t num_nulls=0);

  /**
   * @defgroup AbstractSegment interface
   * @{
   */

  AllTypeVariant operator[](const ChunkOffset chunk_offset) const final;

  // TODO this returns teh base, not the actual value!!
  std::optional<T> get_typed_value(const ChunkOffset chunk_offset) const {
    // performance critical - not in cpp to help with inlining
    
    // Look up dictionary index (ValueID) from compressed attribute vector
    const auto base_idx = reconstruction_list->at(chunk_offset);
    if (base_idx == null_value_id()){
      // requested value is a NULL
      return std::nullopt;
    }
    // Not null, reconstruct the value 
    return get(chunk_offset);
  }

  ChunkOffset size() const final;

  std::shared_ptr<AbstractSegment> copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const final;

  size_t memory_usage(const MemoryUsageCalculationMode mode) const final;
  /**@}*/

  /**
   * @defgroup AbstractEncodedSegment interface
   * @{
   */
  std::optional<CompressedVectorType> compressed_vector_type() const final { return std::nullopt; };
  /**@}*/

  /**
   * @defgroup BaseGddSegment interface
   * @{
   */
  EncodingType encoding_type() const final { return EncodingType::GDD; };


  ValueID null_value_id() const final;

  void segment_vs_value_table_scan(
    const PredicateCondition& condition, 
    const AllTypeVariant& query_value, 
    const ChunkID chunk_id, 
    RowIDPosList& matches,
    const std::shared_ptr<const AbstractPosList>& position_filter) const ;
  
  // Add all rows to the matches, optionally without NULLs
  void _all_to_matches(const ChunkID& chunk_id, RowIDPosList& matches, bool include_nulls=false) const;

  /**@}*/


  private:

  // GDD bases
  const std::shared_ptr<const std::vector<T>> bases; 
  // GDD deviations
  const std::shared_ptr<const DeviationsCV> deviations; 
  // Which base is used for the ith deviation to reconstruct the original value
  const std::shared_ptr<const compact::vector<size_t>> reconstruction_list; 
  // Minimum and maximum value that appears in this segment
  const T segment_min, segment_max; 
  // Number of NULLs in this segment
  const size_t num_nulls; 

  T get(const ChunkOffset& chunk_offset) const ;

};

//EXPLICITLY_DECLARE_DATA_TYPES(GddSegmentV1Fixed);
extern template class GddSegmentV1Fixed<int32_t>;
extern template class GddSegmentV1Fixed<int64_t>;

}  // namespace opossum
