"""Bounded, verified payload publication for explicit maintenance operations."""
import hashlib
import os
import tempfile
from pathlib import Path

from ..events import Stopped
from ..downloads.checkpoint import sync_directory


def file_hash(path, stop_event=None):
    digest = hashlib.sha256()
    size = 0
    with Path(path).open('rb') as stream:
        while block := stream.read(1024*1024):
            if stop_event is not None and stop_event.is_set():
                raise Stopped
            digest.update(block)
            size += len(block)
    return size,digest.hexdigest()


def copy_verified(source, destination, expected_hash, expected_size=None, stop_event=None):
    """Hash during the necessary copy; never publish inconsistent evidence."""
    destination = Path(destination)
    if destination.is_file():
        size,digest = file_hash(destination,stop_event)
        if expected_hash and digest == expected_hash and (expected_size is None or size == expected_size):
            return destination
        raise RuntimeError('Merge destination payload conflicts with source evidence: ' + str(destination))
    destination.parent.mkdir(parents=True,exist_ok=True)
    descriptor,temporary = tempfile.mkstemp(prefix='.merge-',dir=destination.parent)
    try:
        digest = hashlib.sha256()
        size = 0
        with os.fdopen(descriptor,'wb') as output, Path(source).open('rb') as stream:
            while block := stream.read(1024*1024):
                if stop_event is not None and stop_event.is_set():
                    raise Stopped
                output.write(block)
                digest.update(block)
                size += len(block)
            output.flush()
            os.fsync(output.fileno())
        if not expected_hash or digest.hexdigest() != expected_hash or (expected_size is not None and size != expected_size):
            raise RuntimeError('Merge source payload does not match recorded hash/size: ' + str(source))
        os.replace(temporary,destination)
        sync_directory(destination.parent)
        return destination
    finally:
        Path(temporary).unlink(missing_ok=True)


class DiskIdentityMap:
    """Only explicit merge uses this TEMP mapping; memory does not grow with IDs."""
    def __init__(self,database,name):
        self.database,self.name=database,name
        database.execute('CREATE TEMP TABLE IF NOT EXISTS scout_merge_ids(name TEXT,key INTEGER,value INTEGER,PRIMARY KEY(name,key)) WITHOUT ROWID')
        database.execute('DELETE FROM scout_merge_ids WHERE name=?',(name,))

    def __setitem__(self,key,value):
        self.database.execute('INSERT OR REPLACE INTO scout_merge_ids VALUES(?,?,?)',(self.name,key,value))

    def get(self,key,default=None):
        row=self.database.execute('SELECT value FROM scout_merge_ids WHERE name=? AND key=?',(self.name,key)).fetchone()
        return row[0] if row else default

    def __getitem__(self,key):
        value=self.get(key)
        if value is None:
            raise KeyError(key)
        return value

    def __contains__(self,key):
        return self.get(key) is not None
