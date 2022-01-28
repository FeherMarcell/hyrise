#pragma once

#include <memory>
#include <string>
#include <vector>

#include "base_gdd_segment.hpp"
#include "storage/vector_compression/base_compressed_vector.hpp"
#include "types.hpp"

#include "compact_vector.hpp"

namespace opossum {

/**
 * Segment implementing GDD compression in the following way:
 * 
 *  - Fixed 8-bit deviations ('Fix')
 *  - No deviation lookup hashmap ('V1')
 *  - Deviations are not deduplicated ('V1')
 */
template <typename T, typename=std::enable_if_t<encoding_supports_data_type(enum_c<EncodingType, EncodingType::GDD>, hana::type_c<T>)>>
class GddSegmentV1Fixed : public BaseGddSegment {
public:

  // Fixed 1B deviation
  static const auto deviation_bits = 8U;
  // Both bases and deviations are stored in an std::vector
  using BasesType = std::vector<T>;
  using DeviationsType = std::vector<uint8_t>;

  explicit GddSegmentV1Fixed(const std::shared_ptr<const std::vector<T>>& bases,
                            const std::shared_ptr<const std::vector<uint8_t>>& deviations,
                            const std::shared_ptr<const std::vector<size_t>>& reconstruction_list,
                            const T& segment_min=0, const T& segment_max=0, const size_t num_nulls=0);

  /**
   * @defgroup AbstractSegment interface
   * @{
   */

  AllTypeVariant operator[](const ChunkOffset chunk_offset) const final;
  
  std::optional<T> get_typed_value(const ChunkOffset chunk_offset) const {
    // performance critical - not in cpp to help with inlining
    
    // Return nullopt if the value at chunk_offset is a NULL
    if(isnull(chunk_offset)) return std::nullopt;
    // Reconstruct the actual value otherwise
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

  void segment_between_table_scan(
    const PredicateCondition& condition, 
    const AllTypeVariant& left_value, 
    const AllTypeVariant& right_value, 
    const ChunkID chunk_id, 
    RowIDPosList& matches,
    const std::shared_ptr<const AbstractPosList>& position_filter) const ;

  /**@}*/


  // Reconstruct the value at a given position
  T get(const ChunkOffset& chunk_offset) const ;

  // Returns whether the element at chunk_offset is a NULL or not
  bool isnull(const ChunkOffset& chunk_offset) const ;
  
  // Scan a base range with the given predicate and add qualifying RowIDs to 'matches'
  void _scan_base(
    const size_t& base_index, 
    const PredicateCondition& condition, 
    const T& typed_query_value, 
    const ChunkID chunk_id, 
    RowIDPosList& matches,
    const std::shared_ptr<const AbstractPosList>& position_filter) const ;

  // Scan a base range with one of the BETWEEN predicates and add qualifying RowIDs to 'matches'
  void _scan_base_between(
    const size_t base_index, 
    const PredicateCondition& condition, 
    const T& left_query_value, 
    const T& right_query_value, 
    const ChunkID chunk_id, 
    RowIDPosList& matches,
    const std::shared_ptr<const AbstractPosList>& position_filter) const ;
  
  // Add all rows to matches, optionally without NULLs
  void _all_to_matches(
    const ChunkID& chunk_id, 
    RowIDPosList& matches, 
    const std::shared_ptr<const AbstractPosList>& position_filter,
    bool include_nulls=false, 
    bool are_matches_preallocated=false) const;

  // Add rows to matches that use the given base idx
  void _base_idx_to_matches(
    const size_t base_idx, 
    const ChunkID& chunk_id, 
    RowIDPosList& matches,
    const std::shared_ptr<const AbstractPosList>& position_filter) const;

public:

  // Accessors to the internal representation, required by the Iterators
  std::shared_ptr<const BasesType> get_bases() const { return bases; };
  std::shared_ptr<const DeviationsType> get_deviations() const { return deviations; }; 
  std::shared_ptr<const std::vector<size_t>> get_reconstruction_list() const { return reconstruction_list;  }; 


private:


  void verify_position_filter(const ChunkID& chunk_id, const std::shared_ptr<const AbstractPosList>& position_filter) const;
  void verify_matches(RowIDPosList& matches, const std::shared_ptr<const AbstractPosList>& position_filter) const;



private:

  // GDD bases
  const std::shared_ptr<const BasesType> bases; 
  // GDD deviations
  const std::shared_ptr<const DeviationsType> deviations; 
  // Which base is used for the ith deviation to reconstruct the original value
  const std::shared_ptr<const std::vector<size_t>> reconstruction_list; 
  // Minimum and maximum value that appears in this segment
  const T segment_min, segment_max; 
  // Number of NULLs in this segment
  const size_t num_nulls; 
};

//EXPLICITLY_DECLARE_DATA_TYPES(GddSegmentV1Fixed);
extern template class GddSegmentV1Fixed<int32_t>;
extern template class GddSegmentV1Fixed<int64_t>;

}  // namespace opossum
