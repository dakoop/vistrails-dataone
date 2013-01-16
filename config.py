###############################################################################
## VisTrails wrapper for DataONE
## By David Koop, dkoop@poly.edu
##
## Copyright (C) 2012-2013, NYU-Poly.
###############################################################################

#== MN/CN client globals ======================================================

try:
    from core.configuration import ConfigurationObject

    configuration = ConfigurationObject(cn_url=(None, str),
                                        mn_url=(None, str),
                                        cert_file="/tmp/x509up_u501",
                                        key_file="/tmp/x509up_u501",
                                        anonymous=True,
                                        format=(None, str),
                                        submitter=(None, str),
                                        owner=(None, str),
                                        orig_mn=(None, str),
                                        auth_mn=(None, str),
                                        checksum_alg="SHA-1",
                                        )
except ImportError:
    class D1ConfigurationObject(object):
        def __init__(self):
            self.cn_url = None
            self.mn_url = None
            self.cert_file = "/tmp/x509up_u501"
            self.key_file = "/tmp/x509up_u501"
            self.anonymous = True
            self.format = None
            self.submitter = None
            self.owner = None
            self.orig_mn = None
            self.auth_mn = None
            self.checksum_alg = "SHA-1"

        def check(self, attr):
            if hasattr(self, attr) and getattr(self, attr) is not None:
                return True

    configuration = D1ConfigurationObject()
