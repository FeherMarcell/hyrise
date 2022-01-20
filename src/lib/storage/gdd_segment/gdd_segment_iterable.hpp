#pragma once

#include <type_traits>

#include "storage/abstract_segment.hpp"
#include "storage/gdd_segment.hpp"
#include "storage/segment_iterables.hpp"
#include "storage/vector_compression/resolve_compressed_vector_type.hpp"

#include <iostream>

namespace opossum {

template <typename T>
class GddSegmentV1FixedIterable : public PointAccessibleSegmentIterable<GddSegmentV1FixedIterable<T>> {
 public:
  using ValueType = T;
  using SegmentType = GddSegmentV1Fixed<T>;

  explicit GddSegmentV1FixedIterable(const GddSegmentV1Fixed<T>& segment)
      : _segment{segment} {}

  template <typename Functor>
  void _on_with_iterators(const Functor& functor) const {
    // Must call functor with 2 AbstractSegmentIterators (begin, end)
    // These iterators should point to the beginning and end of all segment values 
    std::cout << "GddSegmentIterable with_iterators called (no position filter)" << std::endl;

    _segment.access_counter[SegmentAccessCounter::AccessType::Sequential] += _segment.size();

    //using BasesIteratorType = decltype(_segment.get_bases()->cbegin());
    using ReconListIteratorType = decltype(_segment.get_reconstruction_list()->cbegin());

    auto begin = Iterator<SegmentType, ReconListIteratorType>{
        _segment,
        _segment.get_reconstruction_list()->cbegin(), 
        _segment.null_value_id(), 
        ChunkOffset{0u}
    };

    auto end = Iterator<SegmentType, ReconListIteratorType>{
        _segment,
        _segment.get_reconstruction_list()->cend(), 
        _segment.null_value_id(), 
        static_cast<ChunkOffset>(_segment.size())
    };

    functor(begin, end);
  }

  template <typename Functor, typename PosListType>
  void _on_with_iterators(const std::shared_ptr<PosListType>& position_filter, const Functor& functor) const {
    std::cout << "GddSegmentIterable with_iterators called with position filter of " << position_filter->size() << " elements" << std::endl;
    
    _segment.access_counter[SegmentAccessCounter::access_type(*position_filter)] += position_filter->size();

    using ReconListIteratorType = decltype(_segment.get_reconstruction_list()->cbegin());
    using PosListIteratorType = decltype(position_filter->cbegin());

    auto begin = PointAccessIterator<SegmentType, ReconListIteratorType, PosListIteratorType>{
        _segment,
        _segment.get_reconstruction_list()->cbegin(), 
        _segment.null_value_id(), 
        position_filter->cbegin(),
        position_filter->cbegin()
    };

    auto end = PointAccessIterator<SegmentType, ReconListIteratorType, PosListIteratorType>{
        _segment,
        _segment.get_reconstruction_list()->cend(), 
        _segment.null_value_id(), 
        position_filter->cbegin(),
        position_filter->cend()
    };

    functor(begin, end);
    /*
    resolve_compressed_vector_type(*_segment.attribute_vector(), [&](const auto& vector) {
      using Decompressor = std::decay_t<decltype(vector.create_decompressor())>;
      using DictionaryIteratorType = decltype(_dictionary->cbegin());

      using PosListIteratorType = decltype(position_filter->cbegin());
      auto begin = PointAccessIterator<Decompressor, DictionaryIteratorType, PosListIteratorType>{
          _dictionary->cbegin(), _segment.null_value_id(), vector.create_decompressor(), position_filter->cbegin(),
          position_filter->cbegin()};
      auto end = PointAccessIterator<Decompressor, DictionaryIteratorType, PosListIteratorType>{
          _dictionary->cbegin(), _segment.null_value_id(), vector.create_decompressor(), position_filter->cbegin(),
          position_filter->cend()};
      functor(begin, end);
    });
    */
  }

  size_t _on_size() const { return _segment.size(); }

private:
  const GddSegmentV1Fixed<T>& _segment;


 private:
  
  template <typename SegmentType, typename ReconListIteratorType>
  class Iterator : public AbstractSegmentIterator<Iterator<SegmentType, ReconListIteratorType>, SegmentPosition<T>> {
   public:

    // These 2 using declarations are needed from AnySegmentIterable
    using ValueType = T;
    using IterableType = GddSegmentV1FixedIterable<T>;

    Iterator(
      const SegmentType& segment, 
      ReconListIteratorType recon_list_it, 
      ValueID null_value_id, ChunkOffset chunk_offset)
        : __segment{segment},
          _recon_list_it{std::move(recon_list_it)},
          _null_value_id{null_value_id},
          _chunk_offset{chunk_offset} 
        {}

   private:
    // grants the boost::iterator_facade access to the private interface
    // Mandatory methods to implement: https://www.boost.org/doc/libs/1_46_0/libs/iterator/doc/iterator_facade.html
    friend class boost::iterator_core_access;  

    void increment() {
      ++_recon_list_it;
      ++_chunk_offset;
    }

    void decrement() {
      --_recon_list_it;
      --_chunk_offset;
    }

    void advance(std::ptrdiff_t n) {
      _recon_list_it += n;
      _chunk_offset += n;
    }

    bool equal(const Iterator& other) const { return _recon_list_it == other._recon_list_it; }

    std::ptrdiff_t distance_to(const Iterator& other) const { return other._recon_list_it - _recon_list_it; }

    SegmentPosition<T> dereference() const {

      //const auto value_id = static_cast<ValueID>(*_attribute_it);
      const auto base_idx = static_cast<ValueID>(*_recon_list_it);
      const auto is_null = (base_idx == _null_value_id);
      // Current element is a NULL
      if (is_null) return SegmentPosition<T>{T{}, true, _chunk_offset};
      // Not NULL, reconstruct

      //const auto deviation = *(_deviations_begin_it + _chunk_offset);
      //const auto base = *(_bases_begin_it + base_idx);
      return SegmentPosition<T>{__segment.get(_chunk_offset), false, _chunk_offset};
    }

   private:
    
    const SegmentType& __segment; 
    ReconListIteratorType _recon_list_it; // Iterator to the current element of the reconstruction list
    ValueID _null_value_id; // ValueID representing a NULL value 
    ChunkOffset _chunk_offset; // Row index (just an alias of uint32_t)
  };


  template <typename SegmentType, typename ReconListIteratorType, typename PosListIteratorType>
  class PointAccessIterator : public AbstractPointAccessSegmentIterator<
                                  PointAccessIterator<SegmentType, ReconListIteratorType, PosListIteratorType>,
                                  SegmentPosition<T>, PosListIteratorType> {
   public:
    // These 2 using declarations are needed from AnySegmentIterable
    using ValueType = T;
    using IterableType = GddSegmentV1FixedIterable<T>;
    
    PointAccessIterator(const SegmentType& segment, 
                        ReconListIteratorType recon_list_it, 
                        const ValueID null_value_id,
                        PosListIteratorType position_filter_begin,
                        PosListIteratorType position_filter_it)
        : AbstractPointAccessSegmentIterator<
              PointAccessIterator<SegmentType, ReconListIteratorType, PosListIteratorType>, SegmentPosition<T>,
              PosListIteratorType>{std::move(position_filter_begin), std::move(position_filter_it)},
          __segment{segment},
          _recon_list_it{std::move(recon_list_it)},
          _null_value_id{null_value_id}
        {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    SegmentPosition<T> dereference() const {
      const auto& chunk_offsets = this->chunk_offsets();

      //const auto value_id = _attribute_decompressor.get(chunk_offsets.offset_in_referenced_chunk);
      //const auto is_null = (value_id == _null_value_id);
      //if (is_null) return SegmentPosition<T>{T{}, true, chunk_offsets.offset_in_poslist};
      //return SegmentPosition<T>{T{*(_dictionary_begin_it + value_id)}, false, chunk_offsets.offset_in_poslist};
      /*
      const auto base_idx = static_cast<ValueID>(chunk_offsets.offset_in_referenced_chunk);
      const auto is_null = (base_idx == _null_value_id);
      // Current element is a NULL
      if (is_null) return SegmentPosition<T>{T{}, true, chunk_offsets.offset_in_poslist};
      */
      // Not NULL, reconstruct
      return SegmentPosition<T>{__segment.get(chunk_offsets.offset_in_referenced_chunk), false, chunk_offsets.offset_in_poslist};
    }

   private:
    const SegmentType& __segment; 
    ReconListIteratorType _recon_list_it; // Iterator to the current element of the reconstruction list
    ValueID _null_value_id; // ValueID representing a NULL value 

    //DictionaryIteratorType _dictionary_begin_it;
    //ValueID _null_value_id;
    //mutable Decompressor _attribute_decompressor;
  };


  /*
  // AbstractSegmentIterator for non-filtered access to the whole GddSegment
  template <typename BasesIteratorType, typename DeviationsIteratorType, typename ReconListIteratorType>
  class Iterator : public AbstractSegmentIterator<Iterator<BasesIteratorType, DeviationsIteratorType, ReconListIteratorType>, SegmentPosition<T>> {
   public:

    // These 2 using declarations are needed from AnySegmentIterable
    using ValueType = T;
    using IterableType = GddSegmentV1FixedIterable<T>;

    Iterator(
      BasesIteratorType bases_begin_it, 
      DeviationsIteratorType deviations_begin_it, 
      ReconListIteratorType recon_list_it, 
      ValueID null_value_id, ChunkOffset chunk_offset)
        : _bases_begin_it{std::move(bases_begin_it)},
          _deviations_begin_it{std::move(deviations_begin_it)},
          _recon_list_it{std::move(recon_list_it)},
          _null_value_id{null_value_id},
          _chunk_offset{chunk_offset} 
        {}

   private:
    // grants the boost::iterator_facade access to the private interface
    // Mandatory methods to implement: https://www.boost.org/doc/libs/1_46_0/libs/iterator/doc/iterator_facade.html
    friend class boost::iterator_core_access;  

    void increment() {
      ++_recon_list_it;
      ++_chunk_offset;
    }

    void decrement() {
      --_recon_list_it;
      --_chunk_offset;
    }

    void advance(std::ptrdiff_t n) {
      _recon_list_it += n;
      _chunk_offset += n;
    }

    bool equal(const Iterator& other) const { return _recon_list_it == other._recon_list_it; }

    std::ptrdiff_t distance_to(const Iterator& other) const { return other._recon_list_it - _recon_list_it; }

    SegmentPosition<T> dereference() const {

      //const auto value_id = static_cast<ValueID>(*_attribute_it);
      const auto base_idx = static_cast<ValueID>(*_recon_list_it);
      const auto is_null = (base_idx == _null_value_id);
      // Current element is a NULL
      if (is_null) return SegmentPosition<T>{T{}, true, _chunk_offset};
      // Not NULL, reconstruct

      //const auto deviation = *(_deviations_begin_it + _chunk_offset);
      //const auto base = *(_bases_begin_it + base_idx);
      return SegmentPosition<T>{_segment.get(_chunk_offset), false, _chunk_offset};
    }

   private:
    
    BasesIteratorType _bases_begin_it; // Iterator to the bases vector
    DeviationsIteratorType _deviations_begin_it;  // Iterator to the deviations vector
    ReconListIteratorType _recon_list_it; // Iterator to the current element of the reconstruction list
    ValueID _null_value_id; // ValueID representing a NULL value 
    ChunkOffset _chunk_offset; // Row index (just an alias of uint32_t)
  };
  */

  /*
  template <typename Decompressor, typename DictionaryIteratorType, typename PosListIteratorType>
  class PointAccessIterator : public AbstractPointAccessSegmentIterator<
                                  PointAccessIterator<Decompressor, DictionaryIteratorType, PosListIteratorType>,
                                  SegmentPosition<T>, PosListIteratorType> {
   public:
    // These 2 using declarations are needed from AnySegmentIterable
    using ValueType = T;
    using IterableType = GddSegmentV1FixedIterable<T>;
    
    PointAccessIterator(DictionaryIteratorType dictionary_begin_it, const ValueID null_value_id,
                        Decompressor attribute_decompressor, PosListIteratorType position_filter_begin,
                        PosListIteratorType position_filter_it)
        : AbstractPointAccessSegmentIterator<
              PointAccessIterator<Decompressor, DictionaryIteratorType, PosListIteratorType>, SegmentPosition<T>,
              PosListIteratorType>{std::move(position_filter_begin), std::move(position_filter_it)},
          _dictionary_begin_it{std::move(dictionary_begin_it)},
          _null_value_id{null_value_id},
          _attribute_decompressor{std::move(attribute_decompressor)} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    SegmentPosition<T> dereference() const {
      const auto& chunk_offsets = this->chunk_offsets();

      const auto value_id = _attribute_decompressor.get(chunk_offsets.offset_in_referenced_chunk);
      const auto is_null = (value_id == _null_value_id);

      if (is_null) return SegmentPosition<T>{T{}, true, chunk_offsets.offset_in_poslist};

      return SegmentPosition<T>{T{*(_dictionary_begin_it + value_id)}, false, chunk_offsets.offset_in_poslist};
    }

   private:
    DictionaryIteratorType _dictionary_begin_it;
    ValueID _null_value_id;
    mutable Decompressor _attribute_decompressor;
  };
  */
 
};



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
