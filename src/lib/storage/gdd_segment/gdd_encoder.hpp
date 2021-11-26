#pragma once

#include <memory>

#include "storage/base_segment_encoder.hpp"

#include "storage/run_length_segment.hpp"
#include "storage/value_segment.hpp"
#include "storage/value_segment/value_segment_iterable.hpp"
#include "types.hpp"
#include "utils/enum_constant.hpp"

namespace opossum {

class GddEncoder : public SegmentEncoder<GddEncoder> {
 public:
  static constexpr auto _encoding_type = enum_c<EncodingType, EncodingType::GDD>;
  static constexpr auto _uses_vector_compression = false;

  template <typename T>
  std::shared_ptr<AbstractEncodedSegment> _on_encode(const AnySegmentIterable<T> segment_iterable,
                                                     const PolymorphicAllocator<T>& allocator) {
    auto values = std::make_shared<pmr_vector<T>>(allocator);
    auto null_values = std::make_shared<pmr_vector<bool>>(allocator);

    segment_iterable.with_iterators([&](auto it, auto end) {
      // Early out for empty segments, code below assumes it to be non-empty
      if (it == end) {
        return;
      }

      // Init is_current_null such that it does not equal the first entry
      auto current_value = T{};
      auto is_current_null = !it->is_null();

      for (; it != end; ++it) {
        auto segment_value = *it;
        
        values->push_back(segment_value.value());
        null_values->push_back(segment_value.is_null());
      }
    });

    // The resize method of the vector might have overallocated memory - hand that memory back to the system
    values->shrink_to_fit();
    null_values->shrink_to_fit();

    return std::make_shared<GddSegment<T>>(values, null_values);
  }
};

}  // namespace opossum
