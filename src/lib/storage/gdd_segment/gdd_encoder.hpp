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
        }
      }
    });

    
    // TODO handle NULL values!!
    
    using DeviationsVector = compact::vector<unsigned, GddSegmentV1Fixed<T>::deviation_bits>;
    std::vector<T> bases;
    DeviationsVector deviations;
    // base_indexes must be a shared pointer, because we cannot instantiate a compact vector 
    // without specifying the number of bits (which will be done in gdd_lsb::encode)
    std::shared_ptr<compact::vector<size_t>> base_indexes; 

    gdd_lsb::std_bases::encode(dense_values, bases, deviations, base_indexes);
    std::cout  << "Bases: " << bases.size() << " / " << null_values.size() << std::endl;
    const auto gdd_segment = std::make_shared<GddSegmentV1Fixed<T>>(
        std::make_shared<std::vector<T>>(bases), 
        std::make_shared<DeviationsVector>(deviations), 
        base_indexes,
        *(std::min_element(dense_values.begin(), dense_values.end())),
        *(std::max_element(dense_values.begin(), dense_values.end()))
      );
    
    return gdd_segment;
    /*

      // Build dictionary
      auto dictionary = std::make_shared<pmr_vector<T>>(dense_values.cbegin(), dense_values.cend(), allocator);
      std::sort(dictionary->begin(), dictionary->end());
      dictionary->erase(std::unique(dictionary->begin(), dictionary->end()), dictionary->cend());
      dictionary->shrink_to_fit();
      // NULL ValueID = max dictionary index + 1 (=dictionary size)
      const auto null_value_id = static_cast<uint32_t>(dictionary->size());

      std::cout << segment_size << " values, " << (dictionary.size() + (size_t)has_null) << " unique, bases: " << std::endl
      // Profile the dense values
      const auto bases_nums = gdd_lsb::get_bases_num<T>(dense_values, 100);
      auto bases_nums_ptr = std::make_shared<pmr_vector<uint32_t>>(bases_nums.cbegin(), bases_nums.cend(), allocator);

      // make sure that traditional dedup as a corner case is correct
      assert(bases_nums[0] == dictionary->size());
      
      // Find best compression 
      size_t bits = 0U;
      size_t max_compression_bases = bases_nums[bits], max_compression_dev_bits = bits;
      auto best_compression = gdd_lsb::calculate_compression_rate_percent<T>(max_compression_dev_bits, max_compression_bases, dense_values.size());
      //for(size_t i=1 ; i<bases_nums.size() ; ++i){
      ++bits;
      for(; bits<=8 ; ++bits){
        const auto compression = gdd_lsb::calculate_compression_rate_percent<T>(bits, bases_nums[bits], dense_values.size());
        if(compression > best_compression){
          best_compression = compression;
          max_compression_bases = bases_nums[bits];
          max_compression_dev_bits = bits;
        }
      }

      auto uncompressed_attribute_vector = pmr_vector<uint32_t>{null_values.size(), allocator};
      auto values_iter = dense_values.cbegin();
      const auto null_values_size = null_values.size();
      for (auto current_position = size_t{0}; current_position < null_values_size; ++current_position) {
        if (!null_values[current_position]) {
          const auto value_id = _get_value_id(dictionary, *values_iter);
          uncompressed_attribute_vector[current_position] = value_id;
          ++values_iter;
        } else {
          uncompressed_attribute_vector[current_position] = null_value_id;
        }
      }

      // While the highest value ID used for a value is (dictionary->size() - 1), we need to account for NULL values,
      // encoded as (dictionary->size()). Thus, the highest value id seen in the attribute vector is the one encoding
      // NULL.
      const auto max_value_id = null_value_id;

      const auto compressed_attribute_vector = std::shared_ptr<const BaseCompressedVector>(compress_vector(
          uncompressed_attribute_vector, SegmentEncoder<GddEncoder>::vector_compression_type(),
          allocator, {max_value_id}));
      

      // Encode a segment with a pmr_vector<T> as dictionary
      const auto gdd_segment = std::make_shared<GddSegment<T>>(
          dictionary, 
          compressed_attribute_vector, 
          std::make_shared<pmr_vector<uint32_t>>(bases_nums.cbegin(), bases_nums.cend(), allocator)
        );

      const auto orig_data_size = sizeof(T) * null_values.size();
      const auto dict_segment_size = gdd_segment->memory_usage(MemoryUsageCalculationMode::Full);
      const auto dict_comp_rate = 100*(1-(dict_segment_size/((float)orig_data_size)));
      
      //auto log_str = std::to_string(null_values.size()) + " values, " + 
                    std::to_string(dictionary->size()) + " unique, " + 
                    std::to_string(max_compression_bases) + " bases. Dict compression: " + 
                    std::to_string(dict_comp_rate) + 
                    "%, best base compression (at "+std::to_string(max_compression_dev_bits)+" dev bits): " + 
                    std::to_string(best_compression) + "%\n";
      
      
      // Values, Unique values, Cardinality%, Dict%, GD 8 bits bases,GD 8 bits%, GD best bits, GD best bits - bases, GD best bits %
      
      const auto bases_8_bits = bases_nums[8];
      const auto comp_gd_8_bits = gdd_lsb::calculate_compression_rate_percent<T>(8, bases_nums[8], dense_values.size());

      auto log_str = std::to_string(null_values.size()) + "," + 
                    std::to_string(dictionary->size()) + "," + 
                    std::to_string(dictionary->size()/(float)null_values.size()) + "," + 
                    std::to_string(dict_comp_rate) + "," +
                    
                    std::to_string(bases_8_bits) + "," + 
                    std::to_string(comp_gd_8_bits) + "," + 

                    std::to_string(max_compression_dev_bits) + "," + 
                    std::to_string(max_compression_bases) + "," + 
                    std::to_string(best_compression) + "\n";
      std::cout << log_str;

      return gdd_segment;
    */
  }

 private:
  
};

}  // namespace opossum
