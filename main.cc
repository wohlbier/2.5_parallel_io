#include <assert.h>
#include <iostream>

//#include <memoryweb.h>
//#include <distributed.h>
extern "C" {
#include <emu_c_utils/hooks.h>
#include <io.h>
}

#include "types.hh"

void initialize(std::string const & filename, prMatrix_t M,
                Index_t const nnodes, Index_t const nedges)
{
    Index_t tmp;
    FILE *infile = mw_fopen(filename.c_str(), "r", &tmp);
    mw_fread(&tmp, sizeof(Index_t), 1, infile);
    //assert(tmp == nnodes);
    mw_fread(&tmp, sizeof(Index_t), 1, infile);
    //assert(tmp == nedges);

    // thread local storage to read into
    IndexArray_t iL(nedges);
    IndexArray_t jL(nedges);
    mw_fread(reinterpret_cast<void *>(iL.data()),
             sizeof(Index_t), iL.size(), infile);
    mw_fread(reinterpret_cast<void *>(jL.data()),
             sizeof(Index_t), jL.size(), infile);
    mw_fclose(infile);

    // remove edges where i is a row not owned by this nodelet.
    IndexArray_t iL_nl;
    IndexArray_t jL_nl;
    Index_t nedges_nl = 0;

    for (Index_t e = 0; e < iL.size(); ++e)
    {
        Index_t i = iL[e];
        Index_t j = jL[e];
        if (n_map(i) == NODE_ID())
        {
            iL_nl.push_back(i);
            jL_nl.push_back(j);
            ++nedges_nl;
        }
    }

    // build matrix
    IndexArray_t v_nl(iL_nl.size(), 1);
    M->build(iL_nl.begin(), jL_nl.begin(), v_nl.begin(), nedges_nl);
}

int main(int argc, char* argv[])
{
    if (argc != 2)
    {
        std::cerr << "Usage: ./parallel_io input.bin" << std::endl;
        exit(1);
    }

#ifdef __PROFILE__
    hooks_region_begin("2.5_parallel_io");
#endif

    Index_t nnodes, nedges;
    std::string filename = std::string(argv[1]);

    // open file to get number of nodes and edges, then close
    FILE *infile = mw_fopen(filename.c_str(), "r", &nnodes);
    if (!infile)
    {
        fprintf(stderr, "Unable to open file: %s\n", filename.c_str());
        exit(1);
    }
    mw_fread(&nnodes, sizeof(Index_t), 1, infile);
    mw_fread(&nedges, sizeof(Index_t), 1, infile);
    mw_fclose(infile);

    std::cerr << "nnodes: " << nnodes << std::endl;
    std::cerr << "nedges: " << nedges << std::endl;

    // Create a matrix with nnodes rows
    prMatrix_t L = rMatrix_t::create(nnodes);

    // spawn threads on each nodelet to read and build
    for (Index_t i = 0; i < NODELETS(); ++i)
    {
        cilk_migrate_hint(L->row_addr(i));
        cilk_spawn initialize(filename, L, nnodes, nedges);
    }
    cilk_sync;

//    IndexArray_t iL;
//    IndexArray_t jL;
//    Index_t nedgesL = 0;
//    Index_t max_id = 0;
//    Index_t src, dst;
//
//    // read edges in lower triangle of adjacency matrix
//    while (!feof(infile))
//    {
//        fscanf(infile, "%ld %ld\n", &src, &dst);
//        if (src > max_id) max_id = src;
//        if (dst > max_id) max_id = dst;
//
//        if (dst < src)
//        {
//            iL.push_back(src);
//            jL.push_back(dst);
//            ++nedgesL;
//        }
//    }

//    Index_t nnodes = max_id + 1;
//    std::cerr << "num nodes: " << nnodes << std::endl;
//    std::cerr << "num edges: " << nedgesL << std::endl;
//
//    prIndexArray_t riL = rIndexArray_t::create(nedgesL);
//    prIndexArray_t rjL = rIndexArray_t::create(nedgesL);
//
//    // deep copy iL, jL into riL, rjL. each nodelet has entire edge list.
//    for (Index_t i = 0; i < NODELETS(); ++i)
//    {
//        memcpy(riL->data(i), iL.data(), iL.size() * sizeof(Index_t));
//        memcpy(rjL->data(i), jL.data(), jL.size() * sizeof(Index_t));
//    }
//
//
//
//    // clean up auxilliary arrays
//    delete riL;
//    delete rjL;

#ifdef __PROFILE__
    hooks_region_end();
#endif

    return 0;
}
