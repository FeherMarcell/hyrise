#pragma once

#include <functional>
#include <memory>
#include <tuple>
#include <utility>
#include <vector>

#include "abstract_dereferenced_column_table_scan_impl.hpp"

#include "all_type_variant.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

/**
 * @brief Compares one column to a literal (i.e., an AllTypeVariant)
 *
 * - Value segments are scanned sequentially
 * - For dictionary segments, we basically look up the value ID of the constant value in the dictionary
 *   in order to avoid having to look up each value ID of the attribute vector in the dictionary. This also
 *   enables us to detect if all or none of the values in the segment satisfy the expression.
 */
class ColumnVsValueTableScanImpl : public AbstractDereferencedColumnTableScanImpl {
 public:
  ColumnVsValueTableScanImpl(const std::shared_ptr<const Table>& in_table, const ColumnID column_id,
                             const PredicateCondition& init_predicate_condition, const AllTypeVariant& value);

  std::string description() const override;

  const AllTypeVariant value;

 protected:
  void _scan_non_reference_segment(const AbstractSegment& segment, const ChunkID chunk_id, RowIDPosList& matches,
                                   const std::shared_ptr<const AbstractPosList>& position_filter) override;

  void _scan_generic_segment(const AbstractSegment& segment, const ChunkID chunk_id, RowIDPosList& matches,
                             const std::shared_ptr<const AbstractPosList>& position_filter) const;
  void _scan_dictionary_segment(const BaseDictionarySegment& segment, const ChunkID chunk_id, RowIDPosList& matches,
                                const std::shared_ptr<const AbstractPosList>& position_filter);
  
  void _scan_gdd_segment(
    const BaseGddSegment& segment, const ChunkID chunk_id, RowIDPosList& matches,
    const std::shared_ptr<const AbstractPosList>& position_filter);

  void _scan_sorted_segment(const AbstractSegment& segment, const ChunkID chunk_id, RowIDPosList& matches,
                            const std::shared_ptr<const AbstractPosList>& position_filter,
                            const SortMode sort_mode) const;

  /**
   * @defgroup Methods used for handling dictionary segments
   * @{
   */

  ValueID _get_search_value_id(const BaseDictionarySegment& segment) const;

  bool _value_matches_all(const BaseDictionarySegment& segment, const ValueID search_value_id) const;

  bool _value_matches_none(const BaseDictionarySegment& segment, const ValueID search_value_id) const;

  template <typename Functor>
  void _with_operator_for_dict_segment_scan(const Functor& func) const {
    switch (predicate_condition) {
      case PredicateCondition::Equals:
        func(std::equal_to<void>{});
        return;

      case PredicateCondition::NotEquals:
        func(std::not_equal_to<void>{});
        return;

      case PredicateCondition::LessThan:
      case PredicateCondition::LessThanEquals:
        func(std::less<void>{});
        return;

      case PredicateCondition::GreaterThan:
      case PredicateCondition::GreaterThanEquals:
        func(std::greater_equal<void>{});
        return;

      default:
        Fail("Unsupported comparison type encountered");
    }
  }
  /**@}*/

  /**
   * @defgroup Methods used for handling GDD segments
   * @{
   */
  ValueID _gdd_get_search_value_id(const BaseGddSegment& segment) const;

  bool _gdd_value_matches_all(const BaseGddSegment& segment, const ValueID search_value_id) const;

  bool _gdd_value_matches_none(const BaseGddSegment& segment, const ValueID search_value_id) const;
  
  /**@}*/

 private:
  const bool _column_is_nullable;
};

}  // namespace opossum
