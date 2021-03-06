#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Modifications from DataONE CLI cli_client.py code to create reuse
# (don't use a session)
#
# Modifications by David Koop, NYU-Poly, 2013
#  -- removed session references


# This work was created by participants in the DataONE project, and is
# jointly copyrighted by participating institutions in DataONE. For
# more information on DataONE, see our web site at http://dataone.org.
#
#   Copyright ${year}
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

'''
=================

:Synopsis: CN and MN clients of the DataONE Command Line Interface
:Created: 2012-03-21
:Author: DataONE (Pippin)
'''

# Stdlib.
import datetime
import os
import shutil
import string
import sys
import tempfile
import urllib

# DataONE
try:
    import d1_common.types.exceptions
    import d1_common.types.generated.dataoneTypes as dataoneTypes
except ImportError as e:
    sys.stderr.write('Import error: {0}\n'.format(str(e)))
    sys.stderr.write('Try: easy_install DataONE_Common\n')
    raise

try:
    import d1_client
    import d1_client.mnclient
    import d1_client.cnclient

except ImportError as e:
    sys.stderr.write('Import error: {0}\n'.format(str(e)))
    sys.stderr.write('Try: easy_install DataONE_ClientLib\n')
    raise

# Package-specific
from config import configuration
import access_control as access_control_module
import replication_policy as replication_policy_module

REST_Version = 'v1'
REST_URL_Get = 'object'


#== CN/MN client access =======================================================

def get_default_mn_url():
    if configuration.check("mn_url"):
        return configuration.mn_url
    return None

def get_default_cn_url():
    if configuration.check("cn_url"):
        return configuration.cn_url
    return None

def create_d1_mn_client(mn_url=None, cert_file=None, key_file=None):
    if mn_url is None and configuration.check("mn_url"):
        mn_url = configuration.mn_url
    if mn_url is None:
        raise Exception("Must specify member node URL")
    if cert_file is None:
        cert_file = configuration.cert_file
        if key_file is None and configuration.check("key_file"):
            key_file = configuration.key_file
    if key_file is None:
        key_file = cert_file
    my_d1_mn_client = \
        d1_client.mnclient.MemberNodeClient(mn_url, cert_path=cert_file,
                                            key_path=key_file)
    return my_d1_mn_client

def get_d1_mn_client(*args, **kwargs):
    return create_d1_mn_client(*args, **kwargs)
    # if my_d1_client is None:
    #     return create_d1_client()
    # return my_d1_client

def create_d1_cn_client(cn_url=None):
    if cn_url is None and configuration.check("cn_url"):
        cn_url = configuration.cn_url
    if cn_url is None:
        raise Exception("Must specify coordinating node URL")
    my_d1_cn_client = d1_client.cnclient.CoordinatingNodeClient(cn_url)
    return my_d1_cn_client

def get_d1_cn_client(*args, **kwargs):
    return create_d1_cn_client(*args, **kwargs)

#== Session alternatives ======================================================

def create_system_metadata(pid, size, checksum, algorithm=None, format_id=None,
                           access_policy=None, replication_policy=None,
                           submitter=None, owner=None, orig_mn=None,
                           auth_mn=None):
    sysmeta = dataoneTypes.systemMetadata()
    sysmeta.serialVersion = 1
    sysmeta.identifier = pid
    sysmeta.size = size
    sysmeta.checksum = dataoneTypes.checksum(checksum)
    sysmeta.dateUploaded = datetime.datetime.utcnow()
    sysmeta.dateSysMetadataModified = datetime.datetime.utcnow()

    print "utils CONFIGURATION:", id(configuration), configuration

    if algorithm is not None:
        sysmeta.checksum.algorithm = algorithm
    else:
        sysmeta.checksum.algorithm = configuration.checksum_alg
    if format_id is not None:
        sysmeta.formatId = format_id
    else:
        sysmeta.formatId = configuration.format
    if submitter is not None:
        sysmeta.submitter = submitter
    else:
        sysmeta.submitter = configuration.submitter
    if owner is not None:
        sysmeta.rightsHolder = owner
    else:
        sysmeta.rightsHolder = configuration.owner
    if orig_mn is not None:
        sysmeta.originmn = orig_mn
    else:
        sysmeta.originmn = configuration.orig_mn
    if auth_mn is not None:
        sysmeta.authoritativemn = auth_mn
    else:
        sysmeta.authoritativemn = configuration.auth_mn
    if access_policy is not None:
        sysmeta.accessPolicy = access_policy.to_pyxb()
    else:
        sysmeta.accessPolicy = access_control_module.access_control().to_pyxb()
    if replication_policy is not None:
        sysmeta.replicationPolicy = replication_policy.to_pyxb()
    else:
        sysmeta.replicationPolicy = \
            replication_policy_module.replication_policy().to_pyxb()
    return sysmeta

#== FROM cli_util.py ==========================================================

class ComplexPath(object):
    def __init__(self, path):
        self.path = None
        self.formatId = None
        #
        if not path:
            return
        # parts = string.split(strip(path), ';')
        parts = path.strip().split(';')
        for part in parts:
            # keyval = string.split(part, '=', 1)
            keyval = part.split('=', 1)
            if len(keyval) == 1:
                if keyval[0] != '':
                    self.path = keyval[0]
            else:
                # key = strip(keyval[0]).lower()
                key = kevval[0].strip().lower()
                if key.find('format') == 0:
                    # self.formatId = strip(keyval[1])
                    self.formatId = keyval[1].strip()
                else:
                    # print_warn('Unknown keyword: "%s"' % strip(keyval[0]))
                    print 'WARNING: Uknown keyword: "%s"' % keyval[0].strip()

def create_complex_path(path):
    return ComplexPath(path)

# write_file_output is a modified version of cli_util.output
def write_file_output(file_like_object, path):
    '''Display or save file like object'''
    if not path:
        for line in file_like_object:
            print line.rstrip()
    else:
        try:
            object_file = open(expand_path(path), 'wb')
            shutil.copyfileobj(file_like_object, object_file)
            object_file.close()
        except EnvironmentError as (errno, strerror):
            error_message_lines = []
            error_message_lines.append(
              'Could not write to object_file: {0}'.format(path))
            error_message_lines.append(
              'I/O error({0}): {1}'.format(errno, strerror))
            error_message = '\n'.join(error_message_lines)
            raise Exception(error_message)

def expand_path(filename):
    if filename:
        return os.path.expanduser(filename)
    return None

def get_file_size(path):
    with open(expand_path(path), 'r') as f:
        f.seek(0, os.SEEK_END)
        size = f.tell()
    return size


def get_file_checksum(path, algorithm=None, block_size=1024 * 1024):
    if algorithm is None:
        algorithm = configuration.checksum_alg
    h = d1_common.util.get_checksum_calculator_by_dataone_designator(algorithm)
    with open(expand_path(path), 'r') as f:
        while True:
            data = f.read(block_size)
            if not data:
                break
            h.update(data)
    return h.hexdigest()

def create_sysmeta_from_path(pid, path, algorithm=None, **kwargs):
    ''' Create a system meta data object.
    '''
    if pid is None:
        raise Exception('Missing pid')
    if path is None:
        raise Exception('Missing filename')

    path = expand_path(path)
    checksum = get_file_checksum(path, algorithm)
    size = get_file_size(path)
    return create_system_metadata(pid, size, checksum, algorithm,
                                  **kwargs)

#== FROM cli_client.py ========================================================

def create_get_url_for_pid(baseurl, pid):
    return create_url_for_pid(baseurl, 'resolve', pid)

def create_meta_url_for_pid(baseurl, pid):
    return create_url_for_pid(baseurl, 'meta', pid)

def create_url_for_pid(baseurl, action, pid):
    '''  Create a URL for the specified pid.
    '''
    if baseurl:
        endpoint = baseurl
    else:
        raise Exception('You must specify the base URL')
    if not pid:
        raise Exception('You must specify the pid')
    if not action:
        raise Exception('You must specify the action')
    encoded_pid = urllib.quote_plus(pid)
    return '%s/%s/%s/%s' % (endpoint, REST_Version, action, encoded_pid)


def create_resolve_url_for_pid(baseurl, pid):
    '''  Create a URL for the specified pid.
    '''
    if baseurl:
        endpoint = baseurl
    else:
        raise Exception('You must specify the base URL')
    if not pid:
        raise Exception('You must specify the pid')
    encoded_pid = urllib.quote_plus(pid)
    return '%s/%s/resolve/%s' % (endpoint, REST_Version, encoded_pid)


def get_object_by_pid(pid, filename=None, resolve=True, mn_client=None,
                      cn_client=None):
    ''' Create a mnclient and look for the object.  If the object is not found,
        simply return a None, don't throw an exception.  If found, return the
        filename.
    '''
    if pid is None:
        raise Exception('Missing pid')
    # Create member node client and try to get the object.
    if mn_client is None:
        mn_client = get_d1_mn_client()
    try:
        response = mn_client.get(pid)
        if response is not None:
            fname = _get_fname(filename)
            write_file_output(response, fname)
            return fname
    except d1_common.types.exceptions.DataONEException as e:
        if e.errorCode != 404:
            raise Exception(
              'Unable to get resolve: {0}\n{1}'.format(pid, e.friendly_format()))
    if resolve:
        if cn_client is None:
            cn_client = get_d1_cn_client()
        object_location_list = None
        try:
            object_location_list = cn_client.resolve(pid)
            if ((object_location_list is not None)
                and (len(object_location_list.objectLocation) > 0)):
                baseUrl = object_location_list.objectLocation[0].baseURL
                # If there is an object, go get it.
                mn_client = get_d1_mn_client(mn_url=baseUrl)
                response = mn_client.get(pid)
                if response is not None:
                    fname = _get_fname(filename)
                    write_file_output(response, os.path.expanduser(fname))
                    return fname
        except d1_common.types.exceptions.DataONEException as e:
            if e.errorCode != 404:
                raise Exception(
                  'Unable to get resolve: {0}\n{1}'.format(pid, e.friendly_format()))
    # Nope, didn't find anything
    return None


def _get_fname(filename):
    ''' If fname is none, create a name.
    '''
    fname = filename
    if fname is None:
        tmp_flo = tempfile.mkstemp(prefix= 'd1obj-', suffix='.dat')
        os.close(tmp_flo[0])
        fname = tmp_flo[1]
    return fname


def get_baseUrl(nodeId, cn_client=None):
    '''  Get the base url of the given node id.
    '''
    if cn_client is None:
        cn_client = get_d1_cn_client()
    try:
        nodes = cn_client.listNodes()
        for node in list(nodes.node):
            if node.identifier.value() == nodeId:
                return node.baseURL
    except (d1_common.types.exceptions.ServiceFailure) as e:
        raise Exception("Unable to get node list.")
    return None


def get_sysmeta_by_pid(pid, search_mn=False, cn_client=None, mn_client=None):
    '''  Get the system metadata object for this particular pid.
    '''
    if not pid:
        raise Exception('Missing pid')

    sysmeta = None
    try:
        if cn_client is None:
            cn_client = get_d1_cn_client()
        obsolete = True;
        while obsolete:
            obsolete = False;
            sysmeta = cn_client.getSystemMetadata(pid)
            if not sysmeta:
                return None
            if sysmeta.obsoletedBy:
                # msg = ('Object "%s" has been obsoleted by "%s".  '
                #     + 'Would you rather use that?') % (pid, sysmeta.obsoletedBy)
                # if not cli_util.confirm(msg):
                #   break;
                pid = sysmeta.obsoletedBy
                obsolete = True
        return sysmeta
    except d1_common.types.exceptions.DataONEException as e:
        if e.errorCode != 404:
            raise Exception(
              'Unable to get system metadata for: {0}\n{1}'.format(pid, e.friendly_format()))
    # Search the member node?
    if not sysmeta and (search_mn is not None) and search_mn:
        try:
            if mn_client is None:
                mn_client = get_d1_mn_client()
            obsolete = True;
            while obsolete:
                obsolete = False;
                sysmeta = mn_client.getSystemMetadata(pid)
                if not sysmeta:
                    return None
                if sysmeta.obsoletedBy:
                    # msg = ('Object "%s" has been obsoleted by "%s".  '
                    #     + 'Would you rather use that?') % (pid, sysmeta.obsoletedBy)
                    # if not cli_util.confirm(msg):
                    #   break;
                    pid = sysmeta.obsoletedBy
                    obsolete = True
            return sysmeta
        except d1_common.types.exceptions.DataONEException as e:
            if e.errorCode != 404:
                raise Exception(
                  'Unable to get system metadata for: {0}\n{1}'.format(pid, e.friendly_format()))

    return sysmeta


def run_test():
    configuration.mn_url = "https://mn-demo-9.test.dataone.org/knb/d1/mn"
    configuration.cn_url = "https://cn-stage-2.test.dataone.org/cn"
    cn_client = get_d1_cn_client()
    mn_client = get_d1_mn_client()
    fname = "/vistrails/local_packages/dataone/test_data.csv"
    pid = "dakoop_test502"
    sysmeta = create_sysmeta_from_path(pid, fname, format_id="text/csv",
                                       submitter="dakoop", owner="dakoop",
                                       orig_mn=configuration.mn_url, 
                                       auth_mn=configuration.mn_url)
    f = open(fname, 'r')
    retval = mn_client.create(pid, f, sysmeta)
    f.close()

if __name__ == '__main__':
    run_test()
