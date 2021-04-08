#include "hdf5.h"

#include <iostream>
#include <fstream>
#include <string>
#include <cstring>
#include <vector>
#include <map>


#define _HANDLE_ERROR(fun) \
  {					      \
  std::cout << "Error: " << fun << std::endl; \
  exit(1);				      \
  }


void report(std::ofstream & file, const H5_alloc_stats_t & alloc, const hsize_t & acc_file_size)
{
  file << alloc.total_alloc_bytes << ",";
  file << alloc.curr_alloc_bytes  << ",";
  file << alloc.peak_alloc_bytes  << ",";
  file << acc_file_size << "\n";
}

void header(std::ofstream & file)
{
  file << "total_alloc_bytes,curr_alloc_bytes,peak_alloc_bytes,acc_file_size" << std::endl;
}


/*----< set_rawdata_cache() >------------------------------------------------*/
/* This subroutine increases the raw data chunk cache size in hope to improve
 * decompression time. However, experiments show it is not as effective as
 * doing in-memory I/O. However, we observed no significant difference in
 * performance for reading 2579 small-sized datasets.
 */
int set_rawdata_cache(hid_t  fapl_id,
                      size_t rdcc_nslots,  /* Number of slots in hash table */
                      size_t rdcc_nbytes,  /* Size of chunk cache in bytes */
                      double w0)
{
    int err, err_exit=0;

    /* set the raw data chunk cache to improve decompression time */
    int mdc_nelmts;      /* Dummy parameter in API, no longer used by HDF5 */
    err = H5Pget_cache(fapl_id, &mdc_nelmts, &rdcc_nslots, &rdcc_nbytes, &w0);
    if (err < 0) _HANDLE_ERROR("H5Pget_cache")

    /* increasing cache size for write seems no effect */

    /* rdcc_nslots should be a prime number and approximately 100 times number
     * of chunks that can fit in rdcc_nbytes. The default value used by HDF5 is
     * 521. Others can be 10007, 67231. However, experiments show that
     * increasing rdcc_nslots for this read operation actually performs worse.
     */

    err = H5Pset_cache(fapl_id, mdc_nelmts, rdcc_nslots, rdcc_nbytes, w0);
    if (err < 0) _HANDLE_ERROR("H5Pset_cache")
fn_exit:
    return err_exit;
}

struct op_data_t {
  std::vector<hid_t> dset_ids;
};

herr_t op_func(hid_t obj, const char * name, const H5O_info_t * info, void * op)
{
  op_data_t * op_data = (op_data_t*) op;
  
  herr_t err;
  
  if(info->type == H5O_TYPE_DATASET) {
    hid_t dset_id = H5Dopen(obj, name, H5P_DEFAULT);
    hid_t space_id = H5Dget_space(dset_id);
    
    hsize_t dset_dims[2];
    int ndims = H5Sget_simple_extent_dims(space_id, dset_dims, NULL);
    err = H5Sclose(space_id);

    if(dset_dims[0] <= 0) {
      err = H5Dclose(dset_id);
    }
  }
  return 0;
}


int main(int argc, char ** argv)
{
  if(argc != 3) {
    std::cout << "Usage: estimate_data_volume /path/to/file_list.txt /path/to/output.csv" << std::endl;
    exit(1);
  }

  std::ifstream infile (argv[1]);
  std::ofstream outfile(argv[2], std::ios::trunc);
  header(outfile);
  
  std::vector<std::string> hdf5_files;
  for(std::string line; getline(infile, line); ) {
    hdf5_files.push_back(line);
  }

  H5_alloc_stats_t curr_alloc;
  hsize_t curr_file_size = 0;
  hsize_t acc_file_size = 0;

  bool posix_open = true;
  bool in_memory_io = true;
  bool chunk_caching = true;
  size_t raw_chunk_cache_size = 64*1024*1024;
  hid_t file_id, fapl_id;
  herr_t  err;

  if(posix_open == true) {
    if (in_memory_io == true) {
      fapl_id = H5Pcreate(H5P_FILE_ACCESS);
      
      err = H5Pset_fapl_core(fapl_id, 0, 0);
      if(err < 0) _HANDLE_ERROR("H5Pset_fapl_core");

      if(chunk_caching) {
	err = set_rawdata_cache(fapl_id, 521, raw_chunk_cache_size, 1.0);
	if (err < 0) _HANDLE_ERROR("set_rawdata_cache");
      }
      
    }
    else {
      fapl_id = H5P_DEFAULT;
    }
  }
  else {
    fapl_id = H5Pcreate(H5P_FILE_ACCESS);
    if(fapl_id < 0) _HANDLE_ERROR("H5Pcreate");

    err = H5Pset_all_coll_metadata_ops(fapl_id, false);
    if(err < 0) _HANDLE_ERROR("H5Pset_all_coll_metadata_ops");    
  }
  
  H5get_alloc_stats(&curr_alloc);
  report(outfile, curr_alloc, acc_file_size);

  op_data_t op_data;
  std::map<std::string, hid_t> file_ids;
  for(auto i = 0u; i < hdf5_files.size(); i++) {
    file_id = H5Fopen(hdf5_files[i].c_str(), H5F_ACC_RDONLY, fapl_id);
    H5Fget_filesize(file_id, &curr_file_size);
    acc_file_size += curr_file_size;

    err = H5Ovisit(file_id, H5_INDEX_NAME, H5_ITER_NATIVE, op_func, &op_data);
    
    H5get_alloc_stats(&curr_alloc);
    report(outfile, curr_alloc, acc_file_size);
    
    file_ids[hdf5_files[i]] = file_id;
  }

  for(auto fid = file_ids.begin(); fid != file_ids.end(); fid++) {
    err = H5Fclose(fid->second);
    if(err < 0) _HANDLE_ERROR("H5Fclose");
  }

  infile.close();
  outfile.close();
}
