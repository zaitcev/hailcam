# 
# hailcamsnap: Part of "hailcam" that snaps pictures and loads into S3
#
# Copyright (C) 2010 Red Hat, Inc.
#
# requires: boto, iniparse, swiftclient
#
## Parameters in hailcamsnap.ini:
# [snap]
# sleep = 3
# cmd = fswebcam -p BGR24 -d /dev/video0 --no-banner fs0.jpeg
# file = fs0.jpeg (has to be a JPEG file for now; is deleted, careful)
# s3host = niphredil.zaitcev.lan
# s3user = hailcamsnapuser
# s3pass = hailcamsnappass
# s3bucket = hailcamtest
# prefix = stream0/
#
# If the command is a webcam capture, this script may need to run as root.

import sys
import os
import time
import shlex, subprocess
from iniparse import ConfigParser
from ConfigParser import NoSectionError, NoOptionError

from boto.s3.connection import S3Connection
from boto.exception import S3ResponseError
#from swiftclient import Connection, ClientException, HTTPException
# XXX intercept exceptions
from swiftclient import Connection

class SnapScript:

    def __init__(self, cfg):
        self.cfg = cfg

    def fetch(self):
        try:
            os.unlink(self.cfg["file"])
        except OSError:
            pass

        cmd = self.cfg["cmd"]
        args = shlex.split(cmd)
        try:
            rc = subprocess.call(args)
        except OSError, e:
            print >>sys.stderr, "Command", cmd, "failed:", e
            sys.exit(1)

        if rc != 0:
            print >>sys.stderr, "Bad exit code from command", cmd, ":", rc
        # Add a test for the file; old fswebcam often exits with zero on error

        return rc

    def _upload_s3(self):
        tag = str(int(time.time()))

        c = S3Connection(aws_access_key_id=self.cfg["s3user"],
                         aws_secret_access_key=self.cfg["s3pass"],
                         is_secure=False,
                         host=self.cfg["s3host"])
        # socket.error: [Errno 111] Connection refused

        # If we create a bucket here, it will be owned by the uploader, so
        # the expiration process may fail unless it uses same ID/key. XXX
        # Anyhow, make sure hailcampack is running somewhere, or else
        # the below with fail with a missing bucket.
        bucketname = self.cfg["bucket"]
        try:
            bucket = c.get_bucket(bucketname)
        except S3ResponseError, e:
            # code.message is deprecated, spews a warning to stderr
            # print >>sys.stderr, "S3ResponseError:", "code", getattr(e, 'code')
            print >>sys.stderr, "Bucket", bucketname, "access error: %s" % e
            sys.exit(1)

        key = bucket.new_key()

        key.name = self.cfg["prefix"] + '/' + "i" + tag
        mimetype = "image/jpeg"
        headers = { "Content-Type": mimetype }
        # key.set_contents_from_filename(self.cfg["file"])
        fp = open(self.cfg["file"], 'rb')
        key.set_contents_from_file(fp, headers)
        fp.close()
        key.set_acl('public-read')

        # and now we just return and c is garbage-collected.

    def _upload_swift(self):
        tag = str(int(time.time()))

        (tenant, user) = self.cfg["s3user"].split(':')
        auth_url = "http://" + self.cfg["s3host"] + "/v2.0/"

        conn = Connection(auth_url, user, self.cfg["s3pass"],
                          snet=False, tenant_name=tenant, auth_version="2")

        # Not calling conn.put_container() for the same reason of permissions.

        key_name = self.cfg["prefix"] + '/' + "i" + tag
        # XXX set mime type? how? does it exist in CF/Swift?
        fp = open(self.cfg["file"], 'rb')
        conn.put_object(self.cfg["bucket"], key_name, fp)
        fp.close()

    def upload(self):
        if self.cfg["s3mode"] == 'cfk2':
            # Swift with Keystone authentication
            self._upload_swift()
        else:
            # Amazon S3
            self._upload_s3()

# config()

class ConfigError(Exception):
    pass

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
        cfg["cmd"] = cfgpr.get(inisect, "cmd")
        cfg["file"] = cfgpr.get(inisect, "file")
        cfg["s3mode"] = cfgpr.get(inisect, "s3mode")
        cfg["s3host"] = cfgpr.get(inisect, "s3host")
        cfg["s3user"] = cfgpr.get(inisect, "s3user")
        cfg["s3pass"] = cfgpr.get(inisect, "s3pass")
        cfg["bucket"] = cfgpr.get(inisect, "s3bucket")
        cfg["prefix"] = cfgpr.get(inisect, "prefix")
    except NoSectionError:
        # Unfortunately if the file does not exist, we end here.
        raise ConfigError("Unable to open or find section " + inisect)
    except NoOptionError, e:
        raise ConfigError(str(e))

    try:
        cfg["sleepval"] = float(cfg["sleep"])
    except ValueError:
        raise ConfigError("Invalid sleep value " + cfg["sleep"])

    if cfg["s3mode"] == 'cfk2' and len(cfg["s3user"].split(':')) == 1:
        raise ConfigError("Must have a ':' in user " + cfg["s3user"])

    dirname0 = cfg["prefix"]
    cfg["prefix"] = dirname0[:1] + dirname0[1:].rstrip('/')

    return cfg

# main()
# def main(args):

argc = len(sys.argv)
if argc == 1:
    cfgname = "hailcamsnap.ini"
elif argc == 2:
    cfgname = sys.argv[1]
else:
    print >>sys.stderr, "Usage: hailcamsnap [hailcamsnap.ini]"
    sys.exit(1)

try:
    cfg = config(cfgname, "snap")
except ConfigError, e:   # This is our exception. Other types traceback.
    print >>sys.stderr, "Error in config file " + cfgname + ":", e
    sys.exit(1)

t = SnapScript(cfg)
while 1:
    rc = t.fetch()
    if rc == 0:
        t.upload()
    print "Sleeping %(ss)gs" % { 'ss' : cfg["sleepval"] }
    time.sleep(cfg["sleepval"])

## http://utcc.utoronto.ca/~cks/space/blog/python/ImportableMain
#if __name__ == "__main__":
#    main(sys.argv[1:])
