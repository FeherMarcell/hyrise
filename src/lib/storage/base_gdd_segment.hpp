#pragma once

#include <memory>

#include "abstract_encoded_segment.hpp"


namespace opossum {

/**
 * @brief Base class of GddSegment<T> exposing type-independent interface
 */
class BaseGddSegment : public AbstractEncodedSegment {
 public:
  using AbstractEncodedSegment::AbstractEncodedSegment;

  EncodingType encoding_type() const override = 0;

  /**
   * @brief ColumnVsValue TableScan operator
   * 
   */
  virtual void segment_vs_value_table_scan(
    const PredicateCondition& condition, 
    const AllTypeVariant& query_value, 
    const ChunkID chunk_id, 
    RowIDPosList& matches,
    const std::shared_ptr<const AbstractPosList>& position_filter) const = 0;

  /**
   * @brief ColumnBetween TableScan operator
   * 
   */
  virtual void segment_between_table_scan(
    const PredicateCondition& condition, 
    const AllTypeVariant& left_value, 
    const AllTypeVariant& right_value, 
    const ChunkID chunk_id, 
    RowIDPosList& matches,
    const std::shared_ptr<const AbstractPosList>& position_filter) const = 0;

    

  /**
   * @brief Returns encoding specific null value ID
   */
  virtual ValueID null_value_id() const = 0;
};
}  // namespace opossum
