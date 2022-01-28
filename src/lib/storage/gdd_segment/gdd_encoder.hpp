#pragma once

#include <algorithm>
#include <limits>
#include <memory>
#include <iostream>

#include "storage/base_segment_encoder.hpp"
#include "storage/gdd_segment.hpp"
#include "storage/segment_iterables/any_segment_iterable.hpp"
#include "storage/value_segment.hpp"
#include "storage/vector_compression/base_compressed_vector.hpp"
#include "storage/vector_compression/vector_compression.hpp"
#include "types.hpp"
#include "utils/enum_constant.hpp"

#include "gdd_lsb/gdd_lsb.hpp"

namespace opossum {

/**
 * @brief Encodes a segment using dictionary encoding and compresses its attribute vector using vector compression.
 *
 * The algorithm first creates an attribute vector of standard size (uint32_t) and then compresses it
 * using fixed-width integer encoding.
 */
class GddEncoder : public SegmentEncoder<GddEncoder> {
 public:
  static constexpr auto _encoding_type = enum_c<EncodingType, EncodingType::GDD>;
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

    
    //using DeviationsVector = compact::vector<unsigned, GddSegmentV1Fixed<T>::deviation_bits>;
    // Use the same types for bases and deviations as in the GddSegmentV1Fixed!
    std::vector<T> bases;
    std::vector<uint8_t> deviations;
    std::vector<size_t> base_indexes;
    // reconstruction_list must be a shared pointer, because we cannot instantiate a compact vector 
    // without specifying the number of bits (which will be done in gdd_lsb::encode)
    //std::shared_ptr<compact::vector<size_t>> reconstruction_list; 


    // Perform GD encoding of dense_values into bases, deviations and reconstruction list
    //gdd_lsb::std_bases::encode<T, 8U>(dense_values, bases, deviations, reconstruction_list);
    gdd_lsb::std_bases::encode<T, 8U>(dense_values, bases, deviations, base_indexes);

    // In V1 we don't do anything else, e.g. do not deduplicate deviations or precalculate base-devs mappings

    const auto segment_min = *(std::min_element(dense_values.begin(), dense_values.end()));
    const auto segment_max = *(std::max_element(dense_values.begin(), dense_values.end()));
    
    // null_values has an entry for each stored element, which is True for NULLs
    //std::cout << "Bases: " << bases.size() << " / " << null_values.size() << std::endl;
    const auto gdd_segment = std::make_shared<GddSegmentV1Fixed<T>>(
        std::make_shared<decltype(bases)>(bases), 
        std::make_shared<decltype(deviations)>(deviations), 
        std::make_shared<decltype(base_indexes)>(base_indexes),
        segment_min, segment_max,
        num_nulls
      );
    
    return gdd_segment;
  }

 private:
  
};

}  // namespace opossum
