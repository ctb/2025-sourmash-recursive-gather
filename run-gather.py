#! /usr/bin/env python
import sys
import argparse
import os
import time

import sourmash_plugin_branchwater as branch

class Database:
    def __init__(self, *,
                 shortname=None,
                 path=None,
                 dbtype=None,
                 subdb_dict=None):
        self.shortname = shortname
        assert os.path.exists(path)
        self.path = path
        assert dbtype in ('rocksdb', 'collection')
        self.dbtype = dbtype
        self.subdb_dict = subdb_dict or {} # ident to Database
        self.handle = None

    def load(self, ksize, scaled, moltype):
        if self.handle:
            return

        if self.dbtype == 'rocksdb':
            self.handle = branch.api.BranchRevIndex(self.path)
            assert self.handle.ksize() == ksize, (self.handle.ksize(), ksize)
            assert self.handle.moltype() == moltype, (self.handle.moltype(), moltype)
            min_scaled, max_scaled = self.handle.min_max_scaled()
            assert max_scaled <= scaled, (max_scaled, scaled)
        elif self.dbtype == 'collection':
            self.handle = branch.api.api_load_collection(self.path,
                                                         ksize,
                                                         scaled,
                                                         moltype)
            assert len(self.handle) > 0, len(self.handle)
        else:
            raise Exception("unknown dbtype in load")

        print(f"loaded: {len(self.handle)}")
        

    def __str__(self):
        return f"Database({self.shortname}, {self.dbtype}://{self.path}"


    def gather(self, query_obj, threshold_hashes, scaled, csvfile):
        print(f'running gather against {self.shortname}')
        start = time.time()
        assert self.handle, "must call load first"

        if self.dbtype == 'rocksdb':
            self.handle.fastmultigather_against(query_obj,
                                                self.handle.selection(),
                                                threshold_hashes,
                                                csvfile)
        elif self.dbtype == 'collection':
            self.handle.fastmultigather_against(query_obj,
                                                threshold_hashes,
                                                scaled,
                                                csvfile)
                                                
        else:
            raise Exception("unknown dbtype in gather")

        end = time.time()
        print(f"...done! took {end - start:.1f}s")

databases = [Database(shortname='all',
                      path='../chill-filter/prepare-db/plants+animals+gtdb.rocksdb',
                      dbtype='rocksdb'),
             Database(shortname='podar-ref',
                      path='../sourmash/podar-ref.zip',
                      dbtype='collection'),
             ]


def get_db(shortname):
    for db in databases:
        if db.shortname == shortname:
            return db

    raise Exception(f"database '{shortname}' not found")


def main():
    p = argparse.ArgumentParser()
    p.add_argument('query')
    p.add_argument('-d', '--database', required=False, default='all')
    p.add_argument('-k', '--ksize', type=int, default=31)
    p.add_argument('-m', '--moltype', type=str, default='DNA')
    p.add_argument('-s', '--scaled', type=float, default=100_000)
    args = p.parse_args()

    ksize = args.ksize
    moltype = args.moltype
    scaled = int(args.scaled)

    query_obj = branch.api.api_load_collection(args.query, ksize, scaled, moltype)
    assert len(query_obj) == 1, len(query_obj)

    against_db = get_db(args.database)
    print('got:', against_db)
    against_db.load(ksize, scaled, moltype)

    prefix = args.query.split('.')[0]
    outfile = f'{prefix}.gather.csv'
    against_db.gather(query_obj, 0, scaled, outfile)
    assert os.path.exists(outfile)


if __name__ == '__main__':
    sys.exit(main())
