# 
# hailcampack: Part of "hailcam" that manages the stream
#
# Copyright (C) 2010 Red Hat, Inc.
#
# requires: boto, iniparse
#
## Parameters in hailcamsnap.ini:
# [pack]
# sleep = 13
# s3host = niphredil.zaitcev.lan
# s3user = hailcamuser
# s3pass = hailcampass
# s3bucket = hailcamtest
# prefix = stream0/
# maxsize = 12m (in 1024 units)
# expire = 7d (in s(-econd), m(-in), h, d, w(-eek) mandatory units; 0 is never)

# XXX trap the interrupt signal so it does not traceback

import sys
import os
import time
import rfc822
from iniparse import ConfigParser
from ConfigParser import NoSectionError, NoOptionError
from boto.s3.connection import S3Connection, S3ResponseError
# from boto.s3.bucketlistresultset import BucketListResultSet

class KnownKey:
    def __init__(self, name, size, mtime):
        self.name = name
        self.size = size
        self.mtime = mtime

# Attention, this sorts the keys by newest first for the expiration
def KnownKeyCmpDate(a, b):
    return b.mtime - a.mtime

class PackScript:

    def __init__(self, cfg):
        self.cfg = cfg
        self.known_keys = list([])
        self.used_bytes = 0

    def save_to_known(self, bucket, prefix, name):
        if name[0] != 'i':
            return

        # The caller has a key object, so why get_key? Because the keys
        # returned by an S3 listing may not have all attributes and meta.
        # We fetch all the stuff by calling get_key. It's like readdir+stat.
        # XXX Either try/except this, or take a lock against two hailcampacks.
        key = bucket.get_key(prefix + name)

        modified_tuple = rfc822.parsedate_tz(key.last_modified)
        modified_stamp = int(rfc822.mktime_tz(modified_tuple))

        # stream0/i1273784160 image/jpeg None Thu, 13 May 2010 20:56:00 +0000
        # print key.name, key.content_type, key.filename, key.last_modified

        self.known_keys.append(KnownKey(name, key.size, modified_stamp))

    def expire_1(self, bucket, prefix, kkey):
        if kkey.size + self.used_bytes > self.cfg["maxsize"]:
            # print kkey.name, kkey.mtime, kkey.size, self.used_bytes, "Expire"
            self.used_bytes = self.cfg["maxsize"] + 1
            bucket.delete_key(prefix + kkey.name)
        else:
            # print kkey.name, kkey.mtime, kkey.size, self.used_bytes, "Keep"
            self.used_bytes += kkey.size

    # It may be ridiculously inefficient to concatenate strings so much XXX
    def make_index(self, bucket, prefix):
        index_name = "_index.html"    # cannot start with 'i' (image)

        index = ""
        index += "<html>\r\n"
        index += " <head>\r\n"
        index += '  <meta http-equiv="Content-Type"' + \
                       'content="text/html; charset=utf-8" />\r\n'
        index += " </head>\r\n"

        index += " <body>\r\n"

        for kkey in self.known_keys:
            index += '  <img src="' + kkey.name + '" /><br />\r\n'

        index += " </body>\r\n"
        index += "</html>\r\n"

        mimetype = "text/html"
        headers = { "Content-Type": mimetype }

        xkey = bucket.new_key(prefix + index_name)
        xkey.set_contents_from_string(index, headers)
        xkey.set_acl('public-read')

    def scan(self):
        c = S3Connection(aws_access_key_id=self.cfg["s3user"],
                         aws_secret_access_key=self.cfg["s3pass"],
                         is_secure=False,
                         host=self.cfg["s3host"])
        # socket.error: [Errno 111] Connection refused

        bucketname = self.cfg["bucket"]
        try:
            bucket = c.get_bucket(bucketname)
        except S3ResponseError, e:
            # code.message is deprecated, spews a warning to stderr
            # print >>sys.stderr, "S3ResponseError:", "code", getattr(e, 'code')
            print >>sys.stderr, "Bucket", bucketname, "access error: %s" % e
            bucket = None
            # sys.exit(1)

        if bucket == None:
            try:
                bucket = c.create_bucket(bucketname)
            except S3CreateError, e:
                print >>sys.stderr, "Bucket", bucketname, "create error: %s" % e
                sys.exit(1)
            # bucket.set_acl('public-read')

        self.used_bytes = 0
        self.known_keys = list([])

        prefix = cfg["prefix"]
        klist = bucket.list(prefix)
        for bkey in klist:
            self.save_to_known(bucket, prefix, bkey.name[len(prefix):])

        self.known_keys.sort(KnownKeyCmpDate)

        for kkey in self.known_keys:
            self.expire_1(bucket, prefix, kkey)

        self.make_index(bucket, prefix)

# config()

class ConfigError(Exception):
    pass

def config_size(str):
    bytes = 0
    num = 1
    for c in str:
        if num == 1:
            if c.isdigit():
                bytes *= 10
                bytes += int(c)
            else:
                if c == 'k' or c == 'K':
                    bytes *= 1024
                elif c == 'm' or c == 'M':
                    bytes *= 1024 * 1024
                elif c == 'g' or c == 'G':
                    bytes *= 1024 * 1024 * 1024
                else:
                    raise ConfigError('Invalid size ' + 'str')
                num = 0
        else:
            raise ConfigError('Invalid size ' + 'str')
    return bytes

#def config_time(str):
#    return seconds

# This is what cool people would do, but I don't know how to catch
# improper syntax on this case. Se we just use ConfigParser mode.
# from iniparse import INIConfig
# cfgpr = INIConfig(open(cfgname))

def config(cfgname, inisect):
    cfg = { }
    cfgpr = ConfigParser()
    try:
        cfgpr.read(cfgname)
        cfg["sleep"] = cfgpr.get(inisect, "sleep")
        cfg["s3host"] = cfgpr.get(inisect, "s3host")
        cfg["s3user"] = cfgpr.get(inisect, "s3user")
        cfg["s3pass"] = cfgpr.get(inisect, "s3pass")
        cfg["bucket"] = cfgpr.get(inisect, "s3bucket")
        cfg["prefix"] = cfgpr.get(inisect, "prefix")
        maxsize = cfgpr.get(inisect, "maxsize")
        #expire = cfgpr.get(inisect, "expire")
    except NoSectionError:
        # Unfortunately if the file does not exist, we end here.
        raise ConfigError("Unable to open or find section " + inisect)
    except NoOptionError, e:
        raise ConfigError(str(e))

    try:
        cfg["sleepval"] = float(cfg["sleep"])
    except ValueError:
        raise ConfigError("Invalid sleep value " + cfg["sleep"])

    cfg["maxsize"] = config_size(maxsize)
    #cfg["expire"] = config_time(expire)

    if maxsize == 0:
        raise ConfigError("Zero maxsize")

    if maxsize < 4096:
        raise ConfigError("Invalid maxsize value " + cfg["maxsize"])

    return cfg

# main()

argc = len(sys.argv)
if argc == 1:
    cfgname = "hailcampack.ini"
elif argc == 2:
    cfgname = sys.argv[1]
else:
    print >>sys.stderr, "Usage: hailcampack [hailcampack.ini]"
    sys.exit(1)

try:
    cfg = config(cfgname, "pack")
except ConfigError, e:   # This is our exception. Other types traceback.
    print >>sys.stderr, "Error in config file " + cfgname + ":", e
    sys.exit(1)

t = PackScript(cfg)
while 1:
    t.scan()
    print "Sleeping %(ss)gs" % { 'ss' : cfg["sleepval"] }
    time.sleep(cfg["sleepval"])
