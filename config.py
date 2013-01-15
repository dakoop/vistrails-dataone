###############################################################################
## VisTrails wrapper for DataONE
## By David Koop, dkoop@poly.edu
##
## Copyright (C) 2012-2013, NYU-Poly.
###############################################################################

#== MN/CN client globals ======================================================

class D1Configuration(object):
    def __init__(self, configuration=None):
        self.default_cn_url = None
        self.default_mn_url = None
        self.default_cert_file = "/tmp/x509up_u501"
        self.default_key_file = "/tmp/x509up_u501"
        self.default_anonymous = True
        self.default_format = None
        self.default_submitter = None
        self.default_owner = None
        self.default_orig_mn = None
        self.default_auth_mn = None
        self.default_checksum = "SHA-1"

        if configuration is not None:
            self.set_from_configuration(configuration)

    def set_from_configuration(self, configuration):
        if configuration.check('cn_url'):
            self.default_cn_url = configuration.cn_url
        if configuration.check('mn_url'):
            self.default_mn_url = configuration.mn_url
        if configuration.check('cert_file'):
            self.default_cert_file = configuration.cert_file
        if configuration.check('key_file'):
            self.default_key_file = configuration.key_file
        if configuration.check('anonymous'):
            self.default_anonymous = configuration.anonymous
        if configuration.check('format'):
            self.default_format = configuration.format
        if configuration.check('submitter'):
            self.default_submitter = configuration.submitter
        if configuration.check('owner'):
            self.default_owner = configuration.owner
        if configuration.check('orig_mn'):
            self.default_orig_mn = configuration.orig_mn
        if configuration.check('auth_mn'):
            self.default_auth_mn = configuration.auth_mn
        if configuration.check('checksum_alg'):
            self.default_checksum = configuration.checksum_alg

d1_config = None

def create_d1_config(configuration=None):
    global d1_config
    d1_config = D1Configuration(configuration)

def get_d1_config():
    if d1_config is None:
        create_d1_config()
    return d1_config
