#include "gdd_segment.hpp"

#include <vector>
#include <memory>
#include <string>

#include "resolve_type.hpp"

#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"
#include "utils/size_estimation_utils.hpp"

#include "gdd_segment/gdd_lsb/gdd_lsb.hpp"

#include "type_comparison.hpp" // for with_comparator

namespace opossum {
using namespace std;

template <typename T, typename U>
GddSegmentV1Fixed<T, U>::GddSegmentV1Fixed(const std::shared_ptr<const std::vector<T>>& _bases,
                            const std::shared_ptr<const DeviationsCV>& _deviations,
                            const std::shared_ptr<const compact::vector<size_t>>& _base_indexes,
                            const T& min_value, const T& max_value) 
      : BaseGddSegment(data_type_from_type<T>()),
      bases{_bases},
      deviations{_deviations},
      base_indexes{_base_indexes},
      min_value{min_value},
      max_value{max_value}
{
  // NULL is represented by bases.size() in the reconstruction list. 
  // INVALID_VALUE_ID, which is the highest possible number in ValueID::base_type
  // (2^32 - 1), is needed to represent "value not found" in calls to lower_bound/upper_bound.
  // For a GddSegmentV1Fixed of the max size Chunk::MAX_SIZE, those two values overlap.

  Assert(bases->size() < std::numeric_limits<ValueID::base_type>::max(), "Input segment too big");
}


// TODO this returns teh base, not the actual value!!
template <typename T, typename U>
AllTypeVariant GddSegmentV1Fixed<T, U>::operator[](const ChunkOffset chunk_offset) const {
  PerformanceWarning("operator[] used");
  DebugAssert(chunk_offset != INVALID_CHUNK_OFFSET, "Passed chunk offset must be valid.");
  access_counter[SegmentAccessCounter::AccessType::Dictionary] += 1;
  const auto typed_value = get_typed_value(chunk_offset);
  if (!typed_value) {
    return NULL_VALUE;
  }
  return *typed_value;
}



template <typename T, typename U>
std::shared_ptr<AbstractSegment> GddSegmentV1Fixed<T, U>::copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const {
  // TODO use allocator

  auto new_bases = std::make_shared<std::vector<T>>(*bases);
  auto new_deviations = std::make_shared<DeviationsCV>(*deviations);
  auto new_base_indexes = std::make_shared<compact::vector<size_t>>(*base_indexes);
  auto copy = std::make_shared<GddSegmentV1Fixed<T, U>>(
                                                std::move(new_bases), 
                                                std::move(new_deviations), 
                                                std::move(new_base_indexes),
                                                min_value,
                                                max_value
                                              );
  copy->access_counter = access_counter;
  return copy;
}

// TODO
template <typename T, typename U>
size_t GddSegmentV1Fixed<T, U>::memory_usage(const MemoryUsageCalculationMode mode) const {
  
  return 0U;
  /*
  // Calculate the best compression rate at 
  size_t max_compression_bases = _gdd_profile->at(0), max_compression_dev_bits = 0, best_total_data_bits = 0, total_data_bits=0;
  auto best_compression = gdd_lsb::calculate_compression_rate_percent<T>(max_compression_dev_bits, max_compression_bases, _attribute_vector->size(), best_total_data_bits);
  //for(size_t i=1 ; i<bases_nums.size() ; ++i){
  for(size_t i=1 ; i<=8 ; ++i){

    const auto compression = gdd_lsb::calculate_compression_rate_percent<T>(i, _gdd_profile->at(i), _attribute_vector->size(), total_data_bits);
    if(compression > best_compression){
      max_compression_bases = _gdd_profile->at(i);
      max_compression_dev_bits = i;
      best_total_data_bits = total_data_bits;
    }
  }
  // calculate the size
  return ceil(best_total_data_bits / 8.0);
  */
  /*
  const auto common_elements_size = sizeof(*this) + _attribute_vector->data_size();

  if constexpr (std::is_same_v<T, pmr_string>) {
    return common_elements_size + string_vector_memory_usage(*_dictionary, mode);
  }
  return common_elements_size + _dictionary->size() * sizeof(typename decltype(_dictionary)::element_type::value_type);
  */
}

template <typename T, typename U>
T GddSegmentV1Fixed<T, U>::get(const ChunkOffset& chunk_offset) const {
  return gdd_lsb::std_bases::get((size_t)chunk_offset, bases, deviations, base_indexes);
}

template <typename T, typename U>
void GddSegmentV1Fixed<T, U>::segment_vs_value_table_scan(
    const PredicateCondition& condition, 
    const AllTypeVariant& query_value, 
    const ChunkID chunk_id, 
    RowIDPosList& matches,
    const std::shared_ptr<const AbstractPosList>& position_filter) const 
{
  // TODO
  const auto typed_query_value = boost::get<T>(query_value);
  switch(condition){
    case PredicateCondition::Equals:
    {
      // Early out 1: the requested value is outside of the range of this segment
      if (typed_query_value < min_value || typed_query_value > max_value){
        std::cout << typed_query_value <<  " is out of range: " << min_value << " -> " << max_value << endl;
        break;
      }

      // Look for a base
      std::cout << "Total bases: " << bases->size() << endl;
      const auto query_value_base = gdd_lsb::make_base<T, deviation_bits>(typed_query_value);
      std::cout << "Base of " << typed_query_value << ": " << query_value_base << std::endl;
      const auto lower = std::lower_bound(bases->cbegin(), bases->cend(), query_value_base);
      
      // Early out 2: base not found
      if(lower == bases->end()){
        std::cout << "Base not found" << endl;
        break;
      }
      
      // Base found
      const size_t base_idx = std::distance(bases->begin(), lower);
      std::cout << "Base index: " << base_idx << std::endl;

      
      vector<size_t> deviation_indexes;
      { // Find all deviation indexes that use this base (TODO null values!)
        for(auto i=0U ; i<base_indexes->size() ; ++i){
          if(base_indexes->at(i) == base_idx){
            deviation_indexes.push_back(i);
          }
        }
        Assert(!deviation_indexes.empty(), "No deviations for base #"+std::to_string(base_idx));
        std::cout << deviation_indexes.size() << " deviations use base #" << base_idx << endl;
      }
      
      
      { // Reconstruct original values and evaluate predicate to find matches in this segment
        const auto base = bases->at(base_idx);

        // Reserve enough space in matches so we can push_back all indexes without allocating memory in a loop
        matches.reserve(matches.size() + deviation_indexes.size());
        // Convert the condition (enum) to a bool Functor
        with_comparator(condition, [&](auto predicate_comparator) {

          for(const auto& dev_idx : deviation_indexes){
            // If the base is zero we don't need to reconstruct, just use the deviation directly
            const T reconstructed_val = (base == 0) ? 
                                          deviations->at(dev_idx) : 
                                          gdd_lsb::reconstruct_value<T, deviation_bits>(bases->at(base_idx), deviations->at(dev_idx));
            if(predicate_comparator(reconstructed_val, typed_query_value)){
              matches.push_back(RowID{chunk_id, ChunkOffset{(unsigned) dev_idx}});
            }
          } 
        });
      }
      break;
    }
    default: break;
  }
  return;
}


template <typename T, typename U>
ChunkOffset GddSegmentV1Fixed<T, U>::size() const {
  // Number of elements = length of the reconstruction list
  return static_cast<ChunkOffset>(base_indexes->size());
}


template <typename T, typename U>
ValueID GddSegmentV1Fixed<T, U>::null_value_id() const {
  // Last valid base index + 1
  return ValueID{static_cast<ValueID::base_type>(bases->size())};
}

//EXPLICITLY_INSTANTIATE_DATA_TYPES(GddSegmentV1Fixed);
template class GddSegmentV1Fixed<int64_t>;
template class GddSegmentV1Fixed<int32_t>;

}  // namespace opossum
