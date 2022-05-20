#pragma once

#include <type_traits>

#include "storage/abstract_segment.hpp"
#include "storage/gdd_segment_v1_fixed.hpp"
#include "storage/segment_iterables.hpp"
//#include "storage/vector_compression/resolve_compressed_vector_type.hpp"
//#include "../gdd_lsb/gdd_lsb.hpp"

//#include <iostream>

namespace opossum {


template <typename T>
class GddSegmentV1FixedIterable : public PointAccessibleSegmentIterable<GddSegmentV1FixedIterable<T>> {
 public:
  using ValueType = T;
  using SegmentType = GddSegmentV1Fixed<T>;

 private:
  const std::shared_ptr<SegmentType> _segment_ptr;

 public:
  
  explicit GddSegmentV1FixedIterable(const SegmentType& segment)  : 
    _segment_ptr(std::make_shared<SegmentType>(segment)) 
    { }

  template <typename Functor>
  void _on_with_iterators(const Functor& functor) const {
    // Must call 'functor' with 2 AbstractSegmentIterators: (begin, end)
    // These iterators should point to the beginning and end of all segment values 

    // All values in the segment are accessed in order when this method is called

    _segment_ptr->access_counter[SegmentAccessCounter::AccessType::Sequential] += _segment_ptr->size();
    //std::cout << "GddSegmentV1Fixed _on_with_iterators, no position list" << std::endl;
    ////std::cout << "Segment has " << _segment.size() << " elements" << std::endl;

    // Decompress the whole segment and iterate the std::vector<T>
    using ValueIterator = typename std::vector<T>::const_iterator;
    std::vector<T> decompressed_segment;
    _segment_ptr->decompress(decompressed_segment);
    auto begin = Iterator<ValueIterator>{decompressed_segment.cbegin(), ChunkOffset{0u}};
    auto end = Iterator<ValueIterator>{decompressed_segment.cend(), static_cast<ChunkOffset>(decompressed_segment.size())};
    functor(begin, end);
    
    /*
    // Iterate and dereference each value using the segment.get()
    auto begin = Iterator{ _segment, ChunkOffset{0u} };
    auto end = Iterator{ _segment, static_cast<ChunkOffset>(_segment.size()) };
    functor(begin, end);
    */
  }

  template <typename Functor, typename PosListType>
  void _on_with_iterators(const std::shared_ptr<PosListType>& position_filter, const Functor& functor) const {
    // All offsets in position_filter will be accessed by the functor

    //std::cout << "GddSegmentV1Fixed _on_with_iterators with position filter of " << position_filter->size() << " elements" << std::endl;
    _segment_ptr->access_counter[SegmentAccessCounter::access_type(*position_filter)] += position_filter->size();

    using PosListIteratorType = decltype(position_filter->cbegin());

    auto begin = PointAccessIterator<PosListIteratorType>{
        _segment_ptr, 
        position_filter->cbegin(),
        position_filter->cbegin()
    };

    auto end = PointAccessIterator<PosListIteratorType>{
        _segment_ptr, 
        position_filter->cbegin(),
        position_filter->cend()
    };

    functor(begin, end);
  }

  size_t _on_size() const { return _segment_ptr->size(); }

 private:
  
  // Iterate an std::vector<T>
  template <typename ValueIterator>
  class Iterator : public AbstractSegmentIterator<Iterator<ValueIterator>, SegmentPosition<T>> {
   public:
    using ValueType = T;
    using IterableType = GddSegmentV1FixedIterable<T>;

   public:
    // Begin and End Iterator
    explicit Iterator(ValueIterator data_it, ChunkOffset chunk_offset)
        : _chunk_offset{chunk_offset}, _data_it{std::move(data_it)} { }

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    void increment() {
      ++_chunk_offset;
      ++_data_it;
    }

    void decrement() {
      --_chunk_offset;
      --_data_it;
    }

    void advance(std::ptrdiff_t n) {
      _chunk_offset += n;
      _data_it += n;
    }

    bool equal(const Iterator& other) const { return _data_it == other._data_it; }

    std::ptrdiff_t distance_to(const Iterator& other) const {
      return std::ptrdiff_t{other._chunk_offset} - std::ptrdiff_t{_chunk_offset};
    }

    SegmentPosition<T> dereference() const {
      return SegmentPosition<T>{*_data_it, false, _chunk_offset};
    }

   private:
    ChunkOffset _chunk_offset;
    ValueIterator _data_it;
  };

  /*
  class Iterator : public AbstractSegmentIterator<Iterator, SegmentPosition<T>> {
   public:

    // These 2 using declarations are needed from AnySegmentIterable
    using ValueType = T;
    using IterableType = GddSegmentV1FixedIterable<T>;

    explicit Iterator(const SegmentType& segment, ChunkOffset chunk_offset) : 
              _segment{segment}, 
              _chunk_offset{chunk_offset} 
        { }
    
   private:
    // grants the boost::iterator_facade access to the private interface
    // Mandatory methods to implement: https://www.boost.org/doc/libs/1_46_0/libs/iterator/doc/iterator_facade.html
    friend class boost::iterator_core_access;  

    void increment() {
      ++_chunk_offset;
    }

    void decrement() {
      --_chunk_offset;
    }

    void advance(std::ptrdiff_t n) {
      _chunk_offset += n;
    }

    bool equal(const Iterator& other) const { 
      return _chunk_offset == other._chunk_offset; 
    }

    std::ptrdiff_t distance_to(const Iterator& other) const { return other._chunk_offset - _chunk_offset; }

    SegmentPosition<T> dereference() const {

      return _segment.isnull(_chunk_offset) ? 
                SegmentPosition<T>{T{}, true, _chunk_offset} : 
                SegmentPosition<T>{_segment.get(_chunk_offset), false, _chunk_offset};
    }

   private:
    const SegmentType& _segment;
    ChunkOffset   _chunk_offset; // Row index (just an alias of uint32_t)
  };
  */

  template <typename PosListIteratorType>
  class PointAccessIterator : public AbstractPointAccessSegmentIterator<
                                  PointAccessIterator<PosListIteratorType>,
                                  SegmentPosition<T>, PosListIteratorType> {
   public:
    // These 2 using declarations are needed from AnySegmentIterable
    using ValueType = T;
    using IterableType = GddSegmentV1FixedIterable<T>;
    
    PointAccessIterator(const std::shared_ptr<SegmentType> segment_ptr, 
                        PosListIteratorType position_filter_begin,
                        PosListIteratorType position_filter_it)
        : AbstractPointAccessSegmentIterator<PointAccessIterator<PosListIteratorType>, SegmentPosition<T>, PosListIteratorType>
              {std::move(position_filter_begin), std::move(position_filter_it)},
              _segment_ptr{segment_ptr}
        { 
        }

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    SegmentPosition<T> dereference() const {
      const auto& chunk_offsets = this->chunk_offsets();


      return _segment_ptr->isnull(chunk_offsets.offset_in_referenced_chunk) ? 
                SegmentPosition<T>{T{}, true, chunk_offsets.offset_in_poslist} : 
                SegmentPosition<T>{_segment_ptr->get(chunk_offsets.offset_in_referenced_chunk), false, chunk_offsets.offset_in_poslist};
    }

   private:
    const std::shared_ptr<SegmentType> _segment_ptr;
  };
 
};

/*
template <typename T>
class GddSegmentV1FixedIterable : public PointAccessibleSegmentIterable<GddSegmentV1FixedIterable<T>> {
 public:
  using ValueType = T;
  using SegmentType = GddSegmentV1Fixed<T>;

 private:
  const GddSegmentV1Fixed<T>& _segment;
  std::shared_ptr<const compact::vector<T, GddSegmentV1Fixed<T>::base_bits>> _bases_ptr;
  std::shared_ptr<const std::vector<uint8_t>> _devs_ptr;
  std::shared_ptr<const compact::vector<size_t>> _recon_list_ptr;

 public:
  
  explicit GddSegmentV1FixedIterable(const GddSegmentV1Fixed<T>& segment)
      : 
      _segment{segment},
      _bases_ptr{segment.get_bases()},
      _devs_ptr{segment.get_deviations()},
      _recon_list_ptr{segment.get_reconstruction_list()}
    { }

  template <typename Functor>
  void _on_with_iterators(const Functor& functor) const {
    // Must call functor with 2 AbstractSegmentIterators (begin, end)
    // These iterators should point to the beginning and end of all segment values 
    ////std::cout << "GddSegmentIterable with_iterators called (no position filter)" << std::endl;

    _segment.access_counter[SegmentAccessCounter::AccessType::Sequential] += _segment.size();

    //using BasesIteratorType = decltype(_segment.get_bases()->cbegin());
    ////std::cout << "Segment has " << _segment.size() << " elements" << std::endl;

    
    auto bases_it = _bases_ptr->cbegin();
    auto devs_it = _devs_ptr->cbegin();
    auto recon_it_begin = _recon_list_ptr->cbegin();
    auto recon_it_end = _recon_list_ptr->cend();
    
    using BasesType = decltype(bases_it);
    using DevsType = decltype(devs_it);
    using ReconType = decltype(recon_it_begin);

    auto begin = Iterator<BasesType, DevsType, ReconType>{
        bases_it, devs_it,
        recon_it_begin,
        _segment.null_value_id(),
        ChunkOffset{0u}
    };

    auto end = Iterator<BasesType, DevsType, ReconType>{
        bases_it, devs_it, 
        recon_it_end,
        _segment.null_value_id(),
        static_cast<ChunkOffset>(_segment.size())
    };

    functor(begin, end);
  }

  template <typename Functor, typename PosListType>
  void _on_with_iterators(const std::shared_ptr<PosListType>& position_filter, const Functor& functor) const {

    ////std::cout << "GddSegmentIterable with_iterators called with position filter of " << position_filter->size() << " elements" << std::endl;
    _segment.access_counter[SegmentAccessCounter::access_type(*position_filter)] += position_filter->size();

    using PosListIteratorType = decltype(position_filter->cbegin());

    auto bases_it = _bases_ptr->cbegin();
    auto devs_it = _devs_ptr->cbegin();
    auto recon_it = _recon_list_ptr->cbegin();
    
    using BasesType = decltype(bases_it);
    using DevsType = decltype(devs_it);
    using ReconType = decltype(recon_it);



    auto begin = PointAccessIterator<BasesType, DevsType, ReconType, PosListIteratorType>{
        bases_it, devs_it, recon_it, _segment.null_value_id(),
        position_filter->cbegin(),
        position_filter->cbegin()
    };

    auto end = PointAccessIterator<BasesType, DevsType, ReconType, PosListIteratorType>{
        bases_it, devs_it, recon_it, _segment.null_value_id(),
        position_filter->cbegin(),
        position_filter->cend()
    };

    functor(begin, end);
    
  }

  size_t _on_size() const { return _segment.size(); }


 private:
  
  template <typename BasesType, typename DevsType, typename ReconType>
  class Iterator : public AbstractSegmentIterator<Iterator<BasesType, DevsType, ReconType>, SegmentPosition<T>> {
   public:

    // These 2 using declarations are needed from AnySegmentIterable
    using ValueType = T;
    using IterableType = GddSegmentV1FixedIterable<T>;

    explicit Iterator(BasesType bases_it, DevsType devs_it, ReconType recon_it, ValueID null_value_id, ChunkOffset chunk_offset) : 
              _bases_it{std::move(bases_it)}, 
              _devs_it{std::move(devs_it)}, 
              _recon_it{std::move(recon_it)}, 
              _null_value_id{null_value_id},
              _chunk_offset{chunk_offset} 
        { }
    
   private:
    // grants the boost::iterator_facade access to the private interface
    // Mandatory methods to implement: https://www.boost.org/doc/libs/1_46_0/libs/iterator/doc/iterator_facade.html
    friend class boost::iterator_core_access;  

    void increment() {
      ++_chunk_offset;
      ++_recon_it;
    }

    void decrement() {
      --_chunk_offset;
      --_recon_it;
    }

    void advance(std::ptrdiff_t n) {
      _chunk_offset += n;
      _recon_it += n;
    }

    bool equal(const Iterator& other) const { 
      return _recon_it == other._recon_it; 
    }

    std::ptrdiff_t distance_to(const Iterator& other) const { return other._recon_it - _recon_it; }

    SegmentPosition<T> dereference() const {
      const auto base_idx = *(_recon_it);

      const auto is_null = (static_cast<ValueID>(base_idx) == _null_value_id);
      if (is_null) {
        return SegmentPosition<T>{T{}, true, _chunk_offset};
      }

      // Not null, reconstruct value
      const T base = *(_bases_it + base_idx);
      const auto dev = *(_devs_it + _chunk_offset);
      const T value = gdd_lsb::reconstruct_value<T, 8U>(base, dev);

      return SegmentPosition<T>{value, false, _chunk_offset};
    }

   private:
    BasesType _bases_it;
    DevsType _devs_it;
    ReconType _recon_it;
    ValueID _null_value_id;
    ChunkOffset _chunk_offset; // Row index (just an alias of uint32_t)
  };

  template <typename BasesType, typename DevsType, typename ReconType, typename PosListIteratorType>
  class PointAccessIterator : public AbstractPointAccessSegmentIterator<
                                  PointAccessIterator<BasesType, DevsType, ReconType, PosListIteratorType>,
                                  SegmentPosition<T>, PosListIteratorType> {
   public:
    // These 2 using declarations are needed from AnySegmentIterable
    using ValueType = T;
    using IterableType = GddSegmentV1FixedIterable<T>;
    
    PointAccessIterator(BasesType bases_it, DevsType devs_it, ReconType recon_it, ValueID null_value_id, 
                        PosListIteratorType position_filter_begin,
                        PosListIteratorType position_filter_it)
        : AbstractPointAccessSegmentIterator<
              PointAccessIterator<BasesType, DevsType, ReconType, PosListIteratorType>, SegmentPosition<T>,
              PosListIteratorType>{std::move(position_filter_begin), std::move(position_filter_it)},
          _bases_it{std::move(bases_it)}, 
          _devs_it{std::move(devs_it)}, 
          _recon_it{std::move(recon_it)}, 
          _null_value_id{null_value_id}
        { }

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    SegmentPosition<T> dereference() const {
      const auto& chunk_offsets = this->chunk_offsets();

      const auto base_idx = *(_recon_it + chunk_offsets.offset_in_referenced_chunk);
      const auto is_null = (static_cast<ValueID>(base_idx) == _null_value_id);

      if (is_null) return SegmentPosition<T>{T{}, true, chunk_offsets.offset_in_poslist};
      
      // Not null, reconstruct value
      const T base = *(_bases_it + base_idx);
      const auto dev = *(_devs_it + chunk_offsets.offset_in_referenced_chunk);
      const T value = gdd_lsb::reconstruct_value<T, 8U>(base, dev);
      return SegmentPosition<T>{value, false, chunk_offsets.offset_in_poslist};
    }

   private:
    BasesType _bases_it;
    DevsType _devs_it;
    ReconType _recon_it;
    ValueID _null_value_id;
  };
 
};
*/


template <typename T>
struct is_gdd_segment_iterable {
  static constexpr auto value = false;
};

template <template <typename T> typename Iterable, typename T>
struct is_gdd_segment_iterable<Iterable<T>> {
  static constexpr auto value = std::is_same_v<GddSegmentV1FixedIterable<T>, Iterable<T>>;
};

template <typename T>
inline constexpr bool is_gdd_segment_iterable_v = is_gdd_segment_iterable<T>::value;

}  // namespace opossum
