
#include <vector>
#include <type_traits>
#include "compact_vector.hpp"

namespace gdd_lsb
{

    using namespace std;

    enum SAMPLE_MODE {
        FIRST_SEQUENTIAL = 1, // Use elements from the beginning of the data vector
        RANDOM = 2 // Use a random sample of the data vector (slower but less biased)
    };

    /**
     * Calculate the number of unique bases for every number of bits as deviation.
     * 
     */
    template<typename T>
    std::vector<uint32_t> get_bases_num(
        const std::vector<T>& data, 
        const uint8_t sample_percent=10, 
        const SAMPLE_MODE mode=SAMPLE_MODE::FIRST_SEQUENTIAL)
    {
        assert(sample_percent <= 100);
        const auto data_size_bits = sizeof(T) * 8;

        // Preallocate results vector
        std::vector<uint32_t> results(data_size_bits);

        // Select which data items will be used
        vector<uint32_t> data_idxs;
        const auto sample_size = (size_t) ceil(data.size() * (sample_percent / (double)100));
        if(sample_size == data.size()){
            // Add all
            data_idxs.resize(data.size());
            for(auto i=0 ; i<data_idxs.size() ; ++i){ data_idxs[i] = i; }
        }
        else{
            // List of data vector indexes that we'll use for the evaluation
            data_idxs.resize(sample_size);
            switch(mode){
                case SAMPLE_MODE::FIRST_SEQUENTIAL:
                {
                    // First items
                    for(auto i=0 ; i<sample_size ; ++i){
                        data_idxs[i] = i;
                    }
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
        unsigned long mask = (1UL << data_size_bits) - 1UL;
        T base = 0;
        
        // Avoid allocating a new vector every time
        vector<T> bases;
        for(auto i=0 ; i<data_size_bits ; ++i){
            // produce unique bases by removing the last 'i' bits 
            // Instead of cutting off bits and moving things around, we simply set the 
            // last 'i' bits of the data values to zero, and count the unique  
            
            // Clear the bases vector
            bases.clear();
            // Optimistic resize to max possible size
            bases.resize(sample_size);

            size_t base_idx = 0;

            //cout <<  i << " bits starting, mask: " << bitset<data_size_bits>(mask) << endl;
            for(auto idx : data_idxs){
                // zero out the last bits with the current mask
                base = ((unsigned)data[idx]) & mask;
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
     * Calculate the compression rate if the first 'base_bits' bits are kept in the base.
     * Sums the total length of bases, deviations, and base addresses, using the minimum number 
     * of bits for each.
     * 
     * @param deviation_bits Deviation length in bis
     * @param bases_num Number of unique bases
     * @param data_num Number of data elements (to be compressed)
     * @return The compression rate in percentage, negative number means the data became larger
     */
    template<typename T>
    float calculate_compression_rate_percent(const size_t& deviation_bits, const size_t& bases_num, const size_t& data_num){
        const size_t data_size_bits = sizeof(T) * 8;
        const auto total_bases_bits = bases_num * (data_size_bits-deviation_bits);
        // Deviations: i bits for each data element
        const auto total_dev_bits = data_num * deviation_bits;

        base_indexes_bits = bits_needed(bases_num) * data_num;

        return 100*(1- ((total_bases_bits + total_dev_bits + base_indexes_bits)/(float)total_orig_data_bits));
    }

    size_t _bits_needed(uint32_t value){
        int bits = 0;
        for (int bit_test = 16; bit_test > 0; bit_test >>= 1)
        {
            if (value >> bit_test != 0)
            {
                bits += bit_test;
                value >>= bit_test;
            }
        }
        return bits + value;
    }
}
