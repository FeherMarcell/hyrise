#pragma once

#include <algorithm>
#include <limits>
#include <memory>
#include <iostream>

#include "storage/base_segment_encoder.hpp"
#include "storage/gdd_segment_v1_fixed.hpp"
#include "storage/segment_iterables/any_segment_iterable.hpp"
#include "storage/value_segment.hpp"
#include "storage/vector_compression/base_compressed_vector.hpp"
#include "storage/vector_compression/vector_compression.hpp"
#include "types.hpp"
#include "utils/enum_constant.hpp"

#include "../gdd_lsb/gdd_lsb.hpp"

namespace opossum {

/**
 * @brief Encodes a segment using GDD-v1-fixed encoding (1 byte deviation, nothing fancy with the deviations)
 *
 */
class GddV1FixedEncoder : public SegmentEncoder<GddV1FixedEncoder> {
 public:
  static constexpr auto _encoding_type = enum_c<EncodingType, EncodingType::GDDV1Fixed>;
  static constexpr auto _uses_vector_compression = false;  // see base_segment_encoder.hpp for details
  
  

  template <typename T>
  std::shared_ptr<AbstractEncodedSegment> _on_encode(const AnySegmentIterable<T> segment_iterable,
                                                     const PolymorphicAllocator<T>& allocator) {

    // Vectors to gather the input segment's data. This data is used in a later step to
    // construct the actual dictionary and attribute vector.
    std::vector<T> dense_values;    // contains the actual values (no NULLs)
    std::vector<bool> null_values;  // bitmap to mark NULL values
    size_t num_nulls = 0;           // count the NULLs
    
    // Extract the (dense) values and nulls from the input segment
    segment_iterable.with_iterators([&](auto segment_it, const auto segment_end) {
      const auto segment_size = std::distance(segment_it, segment_end);
      
      dense_values.reserve(segment_size);  // potentially overallocate for segments with NULLs
      null_values.resize(segment_size);    // resized to size of segment

      for (auto current_position = size_t{0}; segment_it != segment_end; ++segment_it, ++current_position) {
        const auto segment_item = *segment_it;
        if (!segment_item.is_null()) {
          const auto segment_value = segment_item.value();
          dense_values.push_back(segment_value);
        } else {
          null_values[current_position] = true;
          ++num_nulls;
        }
      }
    });


    // Perform GD encoding of dense_values into bases, deviations and reconstruction list vectors
    std::vector<T> bases;
    std::vector<uint8_t> deviations;
    std::vector<size_t> base_indexes;
    gdd_lsb::std_bases::encode<T, 8U>(dense_values, bases, deviations, base_indexes);

    // Make a minimal size compact vector from base indexes
    // calculate number of bits needed from the size of the bases vector, since we know that all bases
    // are used in base_indexes
    const auto max_base_index = bases.size()-1;
    const auto num_bits = (max_base_index == 0) ? 1 : gdd_lsb::diagnostics::_address_bits(max_base_index);
    compact::vector<size_t> base_indexes_cv(num_bits, base_indexes.size());
    for(auto i=0U ; i<base_indexes.size() ; ++i){
      base_indexes_cv[i] = base_indexes[i];
    }

    // Store the bases in a compact vector of size: sizeof(T)-1 bytes (deviations are fixed 1 byte)
    //const auto base_bits = ;
    compact::vector<T, GddSegmentV1Fixed<T>::base_bits> bases_cv(bases.size());
    for(auto i=0U ; i<bases.size() ; ++i){
      bases_cv[i] = bases[i];
    }

    // In V1 we don't do anything else, e.g. do not deduplicate deviations or precalculate base-devs mappings

    const auto segment_min = *(std::min_element(dense_values.begin(), dense_values.end()));
    const auto segment_max = *(std::max_element(dense_values.begin(), dense_values.end()));
    
    // null_values has an entry for each stored element, which is True for NULLs
    //std::cout << "Bases: " << bases.size() << " / " << null_values.size() << std::endl;
    const auto gdd_segment = std::make_shared<GddSegmentV1Fixed<T>>(
        std::make_shared<decltype(bases_cv)>(bases_cv), 
        std::make_shared<decltype(deviations)>(deviations), 
        std::make_shared<decltype(base_indexes_cv)>(base_indexes_cv),
        segment_min, segment_max,
        num_nulls
      );
    
    return gdd_segment;
  }
  
};

}  // namespace opossum
