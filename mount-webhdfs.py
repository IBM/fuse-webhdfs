#!/usr/bin/env python
from __future__ import print_function, absolute_import, division

import os
import sys
import logging
import pywebhdfs
from datetime import datetime
from errno import ENOENT, ENOSPC
from stat import S_IFDIR, S_IFLNK, S_IFREG
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
import urllib3
urllib3.disable_warnings(urllib3.exceptions.SecurityWarning)

import webhdfs

logger = logging.getLogger('Webhdfs')
CACHE_MAX_SECONDS = 30

class WebHDFS(LoggingMixIn, Operations):
    """
    A simple Webhdfs filesystem.
    """

    def __init__(self):
        self.client = webhdfs.webhdfs_connect()
        self._stats_cache = {}
        self._listdir_cache = {}
        self._enoent_cache = {}

    def _get_listdir(self, path):
        logger.info("List dir %s", path)
        if path in self._listdir_cache:
            ts_delta = datetime.now() - self._listdir_cache[path][0]
            if ts_delta.total_seconds() < CACHE_MAX_SECONDS:
                entries = self._listdir_cache[path][1]
                logger.debug("_get_listdir %s: cached value %s", path, entries)
                return entries
        entries = []
        # logger.info("Listdir: %s", path)
        for s in self.client.list_dir(path)["FileStatuses"]["FileStatus"]:
            sd = webhdfs.webhdfs_entry_to_dict(s)
            # logger.debug("webhdfs_entry_to_dict %s: %s --> %s", sd['name'], s, sd)
            logger.debug("Updating self._stats_cache[%s]", os.path.join(path, sd['name']))
            self._stats_cache[path + '/' + sd['name']] = (datetime.now(), sd)
            entries.append(sd['name'])
        self._listdir_cache[path] = (datetime.now(), entries)
        logger.debug("_get_listdir %s: new value %s", path, entries)
        return entries

    def _get_status(self, path):
        logger.debug("_get_dir_status %s", path)
        if path in self._stats_cache:
            ts_delta = datetime.now() - self._stats_cache[path][0]
            if ts_delta.total_seconds() < CACHE_MAX_SECONDS:
                sd = self._stats_cache[path][1]
                logger.debug("_get_status: path %s --> cached status %s", path, sd)
                return sd
        # logger.info("get_file_dir_status: %s", path)
        s = self.client.get_file_dir_status(path)["FileStatus"]
        sd = webhdfs.webhdfs_entry_to_dict(s)
        logger.debug("_get_status: path %s --> new status %s", path, sd)
        self._stats_cache[path] = (datetime.now(), sd)
        return sd

    def _flush_file_info(self, path):
        if path in self._stats_cache:
            del self._stats_cache[path]
        if path in self._enoent_cache:
            del self._enoent_cache[path]
        dirname = os.path.dirname(path)
        if dirname in self._listdir_cache:
            del self._listdir_cache[dirname]

    def getattr(self, path, fh=None):
        if path in self._enoent_cache:
            ts_delta = datetime.now() - self._enoent_cache[path]
            if ts_delta.total_seconds() < CACHE_MAX_SECONDS:
                raise FuseOSError(ENOENT)
            else:
                del self._enoent_cache[path]
        try:
            st = self._get_status(path)
            return st
        except pywebhdfs.errors.FileNotFound:
            self._enoent_cache[path] = datetime.now()
            raise FuseOSError(ENOENT)

    def readdir(self, path, fh):
        return [u'.', u'..'] + self._get_listdir(path)

    def read(self, path, size, offset, fh):
        logger.info("read: path %s size %d offset %d", path, size, offset)
        if offset >= self._get_status(path)['st_size']:
            data = b''
        else:
            data = self.client.read_file(path, length=size, offset=offset)[:size]
        logger.info("read: path %s result size %d", path, len(data))
        return data

    def mkdir(self, path, mode):
        logger.info("mkdir %s", path)
        return self.client.make_dir(path, permission=mode)

    def create(self, path, mode):
        logger.info("Create %s", path)
        self.client.create_file(path, file_data=None, overwrite=True, permission=755)
        self._flush_file_info(path)
        return 0

    def write(self, path, data, offset, fh):
        st = self._get_status(path)
        logger.info("Writing to %s size %d at offset %d (file size %d)", path, len(data), offset, st['st_size'])
        if offset + len(data) < st['st_size']:
            logger.warning("Can't write in the middle of the file %s. "
                           "Tried to write %d bytes at offset %d < file size %d",
                           path, len(data), offset, st['st_size'])
            raise FuseOSError(ENOSPC)
        if offset > st['st_size']:
            logger.warning("Can't write to %s at offset %d > file size %d", path, offset, st['st_size'])
            raise FuseOSError(ENOSPC)
        data_sub = data[st['st_size'] - offset:]
        # logger.info("Writing %s: %d bytes at offsets %d..%d",
        #             path, len(data_sub), st['st_size'], st['st_size']+len(data_sub))
        self.client.append_file(path, file_data=data_sub, overwrite=True)
        self._flush_file_info(path)
        return len(data)

    def unlink(self, path):
        logger.info("Unlink %s", path)
        self.client.delete_file_dir(path)
        self._flush_file_info(path)

    def destroy(self, path):
        pass

    """
    def chmod(self, path, mode):
        return self.client.chmod(path, mode)

    def chown(self, path, uid, gid):
        return self.client.chown(path, uid, gid)
        
    def readlink(self, path):
        return self.client.readlink(path)

    def rename(self, old, new):
        return self.client.rename(old, self.root + new)

    def rmdir(self, path):
        return self.client.rmdir(path)

    def symlink(self, target, source):
        return self.client.symlink(source, target)

    def truncate(self, path, length, fh=None):
        return self.client.truncate(path, length)

    def utimens(self, path, times=None):
        # Set Access (times[0]) and Modification (times[1]) times 
        return self.client.set_time(path, times)

    """


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('usage: %s <mountpoint>' % sys.argv[0])
        sys.exit(1)

    logging.basicConfig(level=logging.INFO)

    print("Mounting {} at {}".format(webhdfs.cfg['DEFAULT']['HDFS_BASEURL'], sys.argv[1]))
    fuse = FUSE(operations=WebHDFS(), mountpoint=sys.argv[1], foreground=True, nothreads=True)
