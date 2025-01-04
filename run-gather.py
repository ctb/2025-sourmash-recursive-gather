#! /usr/bin/env python
import sys
import argparse
import os
import time
import csv

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

    def get_subdb_names(self, gather_rows):
        names = set()
        for row in gather_rows:
            match_name = row['match_name']
            if match_name in self.subdb_dict:
                names.add((match_name, self.subdb_dict[match_name]))

        return names

###

databases = [Database(shortname='all',
                      path='../chill-filter/prepare-db/plants+animals+gtdb.rocksdb',
                      dbtype='rocksdb',
                      subdb_dict = { 'bacteria and archaea (GTDB rs220)': 'podar-ref' }),
             Database(shortname='podar-ref',
                      path='../sourmash/podar-ref.zip',
                      dbtype='collection'),
             ]


def get_db(shortname):
    for db in databases:
        if db.shortname == shortname:
            return db

    raise Exception(f"database '{shortname}' not found")


def load_gather_csv(csvfile):
    with open(csvfile, newline='') as fp:
        r = csv.DictReader(fp)
        rows = list(r)

    return rows

def replace_row(gather_rows, match_name, new_gather_rows):
    rows = []
    for row in gather_rows:
        if row['match_name'] != match_name:
            rows.append(row)

    rows.extend(new_gather_rows)


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

    gather_rows = load_gather_csv(outfile)

    subdb_names = against_db.get_subdb_names(gather_rows)
    while subdb_names:
        (match_name, dbname) = subdb_names.pop()
        db = get_db(dbname)
        db.load(ksize, scaled, moltype)

        # run gather against database
        outfile = f'{prefix}.x.{dbname}.gather.csv'
        print('running gather:', outfile)
        db.gather(query_obj, 0, scaled, outfile)
        new_gather_rows = load_gather_csv(outfile)

        # update with new databases to search
        subdb_names.update(db.get_subdb_names(new_gather_rows))

        # update gather output with new results
        gather_rows = replace_row(gather_rows, match_name, new_gather_rows)


if __name__ == '__main__':
    sys.exit(main())
