
#pragma once

#include <vector>
#include <type_traits>
#include "compact_vector.hpp"
#include "permute.hpp"

#include <iostream>
#include <chrono>


namespace gdd_lsb
{
    using namespace std;
    using namespace std::chrono;

    template<typename T, unsigned DEV_BITS> 
    static T make_base(const T& value){
        return value >> DEV_BITS;
    }

    template<typename T, unsigned DEV_BITS> 
    static T reconstruct_value(const T& base, const unsigned& deviation){
        return (base << DEV_BITS) | deviation;
    }
    
    namespace std_bases
    {
        template<typename T, unsigned DEV_BITS> 
        static void encode(const std::vector<T>& data, 
                            std::vector<T>& bases_out, 
                            std::vector<uint8_t>& deviations_out,
                            std::vector<size_t>& base_indexes_out)
        {
            {   // Fill deviations compact vector (it uses the last bits, no transform needed)
                deviations_out.resize(data.size());
                size_t dev_idx = 0;
                for(const auto& d : data){
                    // Simply assign the data element to the deviations compact vector, 
                    // it will only store the last 'deviation_bits' bits of it
                    deviations_out[dev_idx++] = d;
                }
            }

            
            {   // Fill bases vector, shift out deviation bits
                bases_out.resize(data.size());
                size_t base_idx = 0;
                for(const auto& d : data){
                    //bases_out[base_idx++] = d >> DEV_BITS;
                    bases_out[base_idx++] = make_base<T, DEV_BITS>(d);
                }
            }
            
            
            {   // Remove duplicate bases and sort
                std::sort(bases_out.begin(), bases_out.end());
                bases_out.erase(std::unique(bases_out.begin(), bases_out.end()), bases_out.cend());
                bases_out.shrink_to_fit();
            }

            //vector<size_t> base_indexes(data.size());
            base_indexes_out.resize(data.size());
            {   // Fill base indexes
                T base;
                size_t base_index_offset = 0;
                for(const auto& el : data){
                    // Generate the base of this data element
                    base = make_base<T, DEV_BITS>(el);

                    // Find base in bases_out and record its index 
                    base_indexes_out[base_index_offset++] = std::distance(
                        bases_out.cbegin(), 
                        std::lower_bound(bases_out.cbegin(), bases_out.cend(), base)
                    );
                }
            }

            /*
            {   // Determine number of bits for base indexes and fill compact vector
                const auto max_base_index = bases_out.size()-1;
                const auto bits_needed = (unsigned) std::ceil(log2(max_base_index + 1U));
                // Now we can instantiate the compact vector
                base_indexes_ptr = std::make_shared<compact::vector<size_t>>(bits_needed);
                // Add base indexes
                base_indexes_ptr->resize(base_indexes.size());
                for(auto i=0U ; i<base_indexes.size() ; ++i){
                    (*base_indexes_ptr)[i] = base_indexes[i];
                }
            }
            */

        }

        

        template<typename T> 
        static T get(const size_t idx, 
                    const std::vector<T>& bases, 
                    const std::vector<uint8_t>& deviations,
                    const shared_ptr<std::vector<size_t>>& base_indexes_ptr)
        {
            
            const auto deviation = deviations[idx];
            const auto base_index = (*base_indexes_ptr)[idx];
            const auto base = bases[base_index];
            // Make space for the deviation (shift left) and add deviation bits
            //return (base << DEV_BITS) | deviation;
            return reconstruct_value<T, 8U>(base, deviation);
        }

        template<typename T, unsigned DEV_BITS> 
        static T get(const size_t idx, 
                    const std::vector<T>& bases, 
                    const compact::vector<unsigned, DEV_BITS>& deviations,
                    const shared_ptr<compact::vector<size_t>>& base_indexes_ptr)
        {
            
            const auto deviation = deviations[idx];
            const auto base_index = (*base_indexes_ptr)[idx];
            const auto base = bases[base_index];
            // Make space for the deviation (shift left) and add deviation bits
            //return (base << DEV_BITS) | deviation;
            return reconstruct_value<T, DEV_BITS>(base, deviation);
        }

        template<typename T, unsigned DEV_BITS> 
        static T get(const size_t idx, 
                    const std::shared_ptr<const std::vector<T>>& bases, 
                    const std::shared_ptr<const compact::vector<unsigned, DEV_BITS>>& deviations,
                    const std::shared_ptr<const compact::vector<size_t>>& base_indexes_ptr)
        {
            //return get<T, DEV_BITS>(idx, (*bases), (*deviations), base_indexes_ptr);
            const auto deviation = (*deviations)[idx];
            const auto base_index = (*base_indexes_ptr)[idx];
            const auto base = (*bases)[base_index];
            // Make space for the deviation (shift left) and add deviation bits
            //return (base << DEV_BITS) | deviation;
            return reconstruct_value<T, DEV_BITS>(base, deviation);
        }

        template<typename T> 
        static T get(const size_t idx, 
                    const std::shared_ptr<const std::vector<T>>& bases, 
                    const std::shared_ptr<const std::vector<uint8_t>>& deviations,
                    const std::shared_ptr<const std::vector<size_t>>& base_indexes_ptr)
        {
            //return get<T, DEV_BITS>(idx, (*bases), (*deviations), base_indexes_ptr);
            const auto deviation = (*deviations)[idx];
            const auto base_index = (*base_indexes_ptr)[idx];
            const auto base = (*bases)[base_index];
            // Make space for the deviation (shift left) and add deviation bits
            //return (base << DEV_BITS) | deviation;
            return reconstruct_value<T, 8U>(base, deviation);
        }

        template<typename T> 
        static T get(const size_t idx, 
                    const std::shared_ptr<const std::vector<T>>& bases, 
                    const std::shared_ptr<const std::vector<uint8_t>>& deviations,
                    const std::shared_ptr<const compact::vector<size_t>>& base_indexes_ptr)
        {
            //return get<T, DEV_BITS>(idx, (*bases), (*deviations), base_indexes_ptr);
            const auto deviation = (*deviations)[idx];
            const auto base_index = (*base_indexes_ptr)[idx];
            const auto base = (*bases)[base_index];
            // Make space for the deviation (shift left) and add deviation bits
            //return (base << DEV_BITS) | deviation;
            return reconstruct_value<T, 8U>(base, deviation);
        }


        template<typename T, unsigned DEV_BITS> 
        static std::vector<T> decode(const std::vector<T>& bases, 
                            const compact::vector<unsigned, DEV_BITS>& deviations,
                            const shared_ptr<std::vector<size_t>>& base_indexes_ptr)
        {
            const auto data_size = base_indexes_ptr->size();
            std::vector<T> result(data_size);
            for(auto i=0U ; i<data_size ; ++i){
                result[i] = get<T, DEV_BITS>(i, bases, deviations, base_indexes_ptr);
            }
            return result;
        }
    }

    namespace compact_bases
    {
        template<typename T, unsigned BASE_BITS, unsigned DEV_BITS> 
        static void encode(const std::vector<T>& data, 
                            compact::vector<T, BASE_BITS>& bases_out, 
                            compact::vector<unsigned, DEV_BITS>& deviations_out,
                            std::vector<size_t>& base_indexes_out)
        {
            {   // Fill deviations compact vector (it uses the last bits, no transform needed)
                deviations_out.resize(data.size());
                size_t dev_idx = 0;
                for(const auto& d : data){
                    // Simply assign the data element to the deviations compact vector, 
                    // it will only store the last 'deviation_bits' bits of it
                    deviations_out[dev_idx++] = d;
                }
            }

            
            if(false){ // Print bases
                cout << "Bases orig:" << endl;
                for(const auto& b : data){
                    cout << bitset<sizeof(T)*8>(b) << endl;
                }
                cout << endl;
            }
            
            // Since compact::vector does not support shrink_to_fit(), we first use an std::vector to
            // collect and deduplicate bases, and allocate a compact::vector only when we know how many
            // bases are there.
            vector<T> bases(data.size());
            {   // Fill bases compact vector, shift out deviation bits
                size_t base_idx = 0;
                //T base;
                for(const auto& d : data){
                    //base = (*reinterpret_cast<const uint64_t*>(&d)) >> DEV_BITS;
                    //base = d >> DEV_BITS;
                    // if T is a signed type, the shift does not clear the high bits, we need to do that manually
                    /*
                    for(auto i=0 ; i<DEV_BITS ; ++i){
                        //cout << "clearing bit " << (data_bits-i) << endl;
                        base &= ~(1UL << (data_bits-i) );
                    }
                    */
                    bases[base_idx++] = d >> DEV_BITS;
                }
            }

            if(false){ // Print bases after shift
                cout << "Bases after shift:" << endl;
                for(const auto& b : bases){
                    cout << bitset<sizeof(T)*8>(b) << endl;
                }
                cout << endl;
            }

            {   // Sort bases and record the sort order in base indexes
                const auto base_permutation = permute::sort_permutation<T>(bases);
                permute::apply_permutation_in_place<T>(bases, base_permutation);

                // Convert permutation to base indexes
                base_indexes_out.resize(base_permutation.size());
                for(auto i=0U ; i<base_permutation.size() ; ++i){
                    base_indexes_out[base_permutation[i]] = i;
                }
            }

            {   // Remove duplicate bases and adjust base indexes
                auto last_idx = bases.size();
                for(auto i=1U ; i<last_idx ; ++i){
                    if(bases[i] == bases[i-1]){
                        // Remove duplicate base 
                        // (they are sorted, duplicates are guaranteed to be consecutive)
                        bases.erase(bases.begin() + i);

                        // Every index that is 'i' or larger in base_indexes_out should be decreased by 1
                        for(auto j=0U ; j<base_indexes_out.size() ; ++j){
                            if(base_indexes_out[j] >= i){
                                --base_indexes_out[j];
                            }
                        }
                        
                        // Adjust last index and loop iterator
                        --last_idx;
                        --i;
                    }
                }
            }

            {   // Copy the resulting bases to a compact::vector
                bases_out.resize(bases.size());
                for(auto i=0U ; i<bases.size() ; ++i){
                    bases_out[i] = bases[i];
                }
            }
        }
        

        template<typename T, unsigned BASE_BITS, unsigned DEV_BITS> 
        static T get(const size_t idx, const compact::vector<T, BASE_BITS>& bases, 
                            const compact::vector<unsigned, DEV_BITS>& deviations,
                            const compact::vector<size_t>& base_indexes){
            
            const auto deviation = deviations[idx];
            const auto base_index = base_indexes[idx];
            const auto base = bases[base_index];
            // Make space for the deviation (shift left) and add deviation bits
            return (base << DEV_BITS) | deviation;
        }

        
        template<typename T, unsigned BASE_BITS, unsigned DEV_BITS> 
        static std::vector<T> decode(const compact::vector<T, BASE_BITS>& bases, 
                            const compact::vector<unsigned, DEV_BITS>& deviations,
                            const compact::vector<size_t>& base_indexes)
        {
            const auto data_size = base_indexes.size();
            std::vector<T> result(data_size);
            for(auto i=0U ; i<data_size ; ++i){
                result[i] = get<T, BASE_BITS, DEV_BITS>(i, bases, deviations, base_indexes);
                /*
                // Get the deviation and base pointed by base_index
                const auto deviation = deviations[i];
                const auto base_index = base_indexes[i];
                const auto base = bases[base_index];
                // Make space for the deviation (shift left) and add deviation bits
                result[i] = (base << DEV_BITS) | deviation;
                */
            }

            return result;
        }

    }
    
    namespace diagnostics
    {
        enum SAMPLE_MODE {
            FIRST_N = 1, // Use elements from the beginning of the data vector
            RANDOM = 2 // Use a random sample of the data vector (slower but less biased)
        };
        /**
         * The following functions help tuning the GDD parameters
         */
    
        /**
         * Calculate the number of deviations and unique deviations per base. 
         * Returns a 2D vector that has one row for each base (starting from base 0), and each row 
         * has 2 elements: total deviations and unique deviations that belong to the base
         */
        template<unsigned DEV_BITS>
        static std::vector<std::vector<size_t>> deviations_per_base(
                            const std::vector<size_t>& base_indexes, 
                            const compact::vector<unsigned, DEV_BITS>& deviations) 
        {
            const auto max_base_idx_it = std::max_element(base_indexes.begin(), base_indexes.end());
            const auto max_base_idx = *max_base_idx_it;

            // Allocate the result
            std::vector<std::vector<size_t>> result(max_base_idx+1);
            vector<size_t> row(2);
            vector<unsigned> devs;
            devs.reserve(deviations.size());
            for(auto base_idx=0U ; base_idx <= max_base_idx ; ++base_idx){
                // Collect all deviations that belong to the current base
                cout << "[#" << base_idx << "] ";
                devs.clear();
                for(auto i=0U ; i<base_indexes.size() ; ++i){
                    if(base_indexes[i] == base_idx){
                        devs.push_back((unsigned)deviations[i]);
                        cout << deviations[i] << " ";
                        //cout << "*";
                    }
                }
                //cout << endl;
                // Count total
                row[0] = devs.size();
                // Count unique
                std::sort(devs.begin(), devs.end());
                devs.erase(unique( devs.begin(), devs.end() ), devs.end());
                row[1] = devs.size();
                cout << " (total: " << row[0] << ", unique: " << row[1] << ")" << endl;
                

                // Add to result
                result[base_idx] = row;
            }
            return result;
        }

        

        /**
         * Calculate the number of unique bases for every number of bits as deviation.
         * 
         */
        template<typename T>
        static std::vector<uint32_t> make_bases_num(
            const std::vector<T>& data, 
            const uint8_t sample_percent=10, 
            const SAMPLE_MODE mode=SAMPLE_MODE::FIRST_N,
            const int8_t max_bits=-1)
        {
            assert(sample_percent <= 100);
            
            const uint8_t data_size_bits = sizeof(T) * 8;
            assert(data_size_bits <= sizeof(unsigned long)*8);

            // Preallocate results vector
            std::vector<uint32_t> results(data_size_bits);

            // Select which data items will be used
            std::vector<uint32_t> data_idxs;

            const size_t sample_size = (size_t) ceil(data.size() * (sample_percent / (double)100));
            if(sample_size == data.size()){
                // Add all
                data_idxs.resize(data.size());
                for(size_t i=0 ; i<data_idxs.size() ; ++i){ data_idxs[i] = i; }
            }
            else{
                // List of data vector indexes that we'll use for the evaluation
                data_idxs.resize(sample_size);
                switch(mode){
                    case SAMPLE_MODE::FIRST_N:
                    {
                        // First items
                        for(size_t i=0 ; i<sample_size ; ++i){ data_idxs[i] = i; }
                        break;
                    }
                    case SAMPLE_MODE::RANDOM:
                    {
                        size_t inserted_num = 0;
                        while(inserted_num != sample_size){
                            auto randidx = rand() % (data.size()-1);
                            const auto range_end = data_idxs.begin()+inserted_num;
                            while(std::find(data_idxs.begin(), range_end, randidx) != range_end){
                                // collision, pick a new number
                                randidx = rand() % (data.size()-1);
                            }
                            data_idxs[inserted_num] = randidx;
                            ++inserted_num;
                        }
                        break;
                    }
                    default: throw std::runtime_error("Unexpected sample mode!");
                }
            }
            
            // Initialize the mask to all ones, and allocate a base that will be re-used in the loop
            // (make sure to use 1UL instead of 1U, so it works with 32-bit and 64-bit types!)
            #pragma GCC diagnostic push
            #pragma GCC diagnostic ignored "-Wshift-count-overflow"
            unsigned long mask = (1UL << data_size_bits) - 1UL;
            #pragma GCC diagnostic pop
            T base = 0;
            
            // Avoid allocating a new vector every time
            std::vector<T> bases;
            size_t base_idx = 0; 
            size_t bits_max = (max_bits < 0) ? data_size_bits : max_bits;
            for(size_t i=0 ; i<bits_max ; ++i){
                
                // Clear the bases vector
                bases.clear();
                // Optimistic resize to max possible size
                bases.resize(sample_size);

                // <<  i << " bits starting, mask: " << bitset<data_size_bits>(mask) << endl;
                
                base_idx = 0;
                // Make bases from the selected data elements
                for(auto idx : data_idxs){
                    // Instead of cutting off bits and moving things around, we simply set the 
                    // last 'i' bits of the data values to zero with the current mask
                    base = ((unsigned) data[idx]) & mask;
                    // Add to bases to the next position
                    bases[base_idx++] = base;
                }

                // Remove duplicates
                sort( bases.begin(), bases.end() );
                bases.erase( unique( bases.begin(), bases.end() ), bases.end() );

                // add the number of unique bases
                results[i] = bases.size();
                
                // Update the bitmask so it clears the next bit from the end
                mask &= ~(1UL << i);
            }
            return results;
        }

        /**
         * Calculates the number of bits required to address an array of the given size.
         * Note: zero is returned if the array is empty or has one element, in 
         * these cases no addressing is necessary.
         * 
         * @param array_size Size of the array we want to address with as few bits as possible
         * @return Smalles number of bits that can address the given array size
         */ 
        static size_t _address_bits(uint32_t array_size){
            if(array_size <= 1){
                return 0;
            }
            int bits = 0;
            for (int bit_test = 16; bit_test > 0; bit_test >>= 1)
            {
                if (array_size >> bit_test != 0)
                {
                    bits += bit_test;
                    array_size >>= bit_test;
                }
            }
            return bits + array_size;
        }

        static size_t __dummy;
        /**
         * Calculate the compression rate with 'deviation_bits' length deviations.
         * Returns a number between 0-1, smaller value means more compression.
         * 
         * @param deviation_bits Deviation length in bis
         * @param bases_num Number of unique bases
         * @param data_num Number of data elements (to be compressed)
         * @param total_compressed_bits_out (optional, out) The total number of bits needed for the compressed representation 
         * @return The compression rate in percentage, negative number means the data became larger
         */
        template<typename T>
        static float calculate_compression_rate(const size_t& deviation_bits, const size_t& bases_num, const size_t& data_num, size_t& total_compressed_bits_out=__dummy){
            const size_t data_size_bits = sizeof(T) * 8;
            const auto total_bases_bits = bases_num * (data_size_bits-deviation_bits);
            const size_t total_orig_data_bits = data_num * data_size_bits;
            // Deviations: i bits for each data element
            const auto total_dev_bits = data_num * deviation_bits;

            const auto base_indexes_bits = _address_bits(bases_num) * data_num;
            total_compressed_bits_out = total_bases_bits + total_dev_bits + base_indexes_bits;

            return total_compressed_bits_out / (float) total_orig_data_bits;
        }

    }
    
}
