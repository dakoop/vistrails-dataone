#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Modifications from DataONE CLI data_package.py code to reuse for
# VisTrails
#
# Modifications by David Koop, NYU-Poly, 2013
#  -- also fixed a few bugs (typos, mismatched arguments)

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
:mod:`data_package`
===================

:Synopsis: Wrapper around a data package
:Created: 2012-03-29
:Author: DataONE (Pippin)
'''


# Stdlib.
import os
import sys
import StringIO
from xml.dom.minidom import parse, parseString #@UnusedImport


# 3rd party
try:
    from rdflib import Namespace, URIRef
    import foresite
    import foresite.utils
except ImportError as e:
    sys.stderr.write('Import error: {0}\n'.format(str(e)))
    sys.stderr.write('  available at: https://foresite-toolkit.googlecode.com/svn/foresite-python/trunk\n')
    raise


# DataONE
# common
try:
    import d1_common.util as util
    from d1_common.types.exceptions import DataONEException
except ImportError as e:
    sys.stderr.write('Import error: {0}\n'.format(str(e)))
    sys.stderr.write('Please install d1_common.\n')
    raise

# vistrails package
import utils
from config import configuration

ALLOWABLE_PACKAGE_SERIALIZATIONS = ('xml', 'pretty-xml', 'n3', 'rdfa', 'json',
                                    'pretty-json', 'turtle', 'nt', 'trix')
RDFXML_FORMATID = 'http://www.openarchives.org/ore/terms'

RDF_NS = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#'
CITO_NS = 'http://purl.org/spar/cito/'
DCTERMS_NS = 'http://purl.org/dc/terms/'


#** DataPackage ***************************************************************

class DataPackage(object):

    def __init__(self, pid=None):
        ''' Create a package
        '''
        self.pid = pid
        #
        # Objects in here a dicts with keywords pid, dirty, obj, meta; which are
        # string, boolean, blob, and pyxb objects respectively.
        self.original_pid = None
        self.sysmeta = None
        self.scimeta = None
        self.scidata_dict = {}
        self.resmap = None


    #== Informational =========================================================

    def is_dirty(self):
        ''' Check to see if anything needs to be saved.
        '''
        if self.pid != self.original_pid:
            return True
        if self.scimeta is not None:
            if (self.scimeta.dirty) and self.scimeta.dirty:
                return True
        if self.scidata_dict is not None:
            for item in self.scidata_dict.values():
                if (item.dirty is not None) and item.dirty:
                    return True
        return False


    #== Manipulation ==========================================================

    def name(self, pid):
        ''' Rename the package
        '''
        self.pid = pid
        # if pid is None:
        #   if cli_util.confirm('Do you really want to clear the name of the package?'):
        #     self.pid = None
        #   else:
        #     raise Exception('Missing the new pid')
        # else:
        #   if self.pid is not None:
        #     print_info('Package name is cleared.')
        #   self.pid = pid


    def load(self):
        ''' Get the object referred to by pid and make sure it is a
            package.
        '''
        if self.pid is None:
            raise Exception('Missing pid')
        sysmeta = utils.get_sysmeta_by_pid(self.pid)
        if not sysmeta:
            raise Exception('Couldn\'t find "%s" in DataONE.' % self.pid)
        if sysmeta.formatId != RDFXML_FORMATID:
            raise Exception('Package must be in RDF/XML format (not "%s").' % \
                              sysmeta.formatId)

        rdf_xml_file = utils.get_object_by_pid(self.pid)
        if not self._parse_rdf_xml(rdf_xml_file):
            raise Exception('Unable to load package "%s".' % self.pid)
        self.original_pid = self.pid
        self.sysmeta = sysmeta

        self.scimeta = self._download_object(self.scimeta)
        loaded_scidata = {}
        for pid, scidata in self.scidata_dict.iteritems():
            loaded_scidata[pid] = self._download_object(scidata)
        self.scidata_dict = loaded_scidata
        return self


    def _parse_rdf_xml(self, xml_file):
        doc = parse(xml_file)
#    print 'doc:\n', doc.toxml()
        self.scimeta = None
        self.scidata_dict = {}
        for desc in doc.getElementsByTagNameNS(RDF_NS, 'Description'):
            documentedBy = desc.getElementsByTagNameNS(CITO_NS, 'isDocumentedBy')
            if documentedBy and len(documentedBy) > 0:
                scidata = DataObject(dirty=False)
                pid_element = desc.getElementsByTagNameNS(DCTERMS_NS, 'identifier')
                if pid_element and pid_element.item(0) and pid_element.item(0).hasChildNodes():
                    scidata.pid = pid_element.item(0).firstChild.nodeValue
                    if len(documentedBy) > 1:
                        print 'Using the first Science Metadata Object for %s' % scidata.pid
                    about_url = documentedBy.item(0).getAttributeNS(RDF_NS, 'resource')
                    scidata.documented_by = about_url
                    self.scidata_dict[scidata.pid] = scidata
            # scimeta?
            elif desc.getElementsByTagNameNS(CITO_NS, 'documents'):
                if self.scimeta:
                    print 'Already have a science metadata object (%s).  Skipping...' % \
                        self.scimeta.pid
                else:
                    self.scimeta = DataObject(dirty=False)
                    pid_element = desc.getElementsByTagNameNS(DCTERMS_NS, 'identifier')
                    if pid_element and pid_element.item(0) and pid_element.item(0).hasChildNodes():
                        self.scimeta.pid = pid_element.item(0).firstChild.nodeValue
        return True


    def save(self, mn_client=None, cn_client=None, **kwargs):
        ''' Save this object referred to by this pid.
        '''
        if self.pid is None:
            raise Exception('Missing pid')

        if mn_client is None:
            mn_client = utils.get_d1_mn_client()
        if cn_client is None:
            cn_client = utils.get_d1_cn_client()

        pkg_xml = self._serialize('xml', mn_client)
        if not pkg_xml:
            raise Exception("Couldn't serialize object.")

        algorithm = configuration.checksum_alg
        hash_fcn = util.get_checksum_calculator_by_dataone_designator(algorithm)
        hash_fcn.update(pkg_xml)
        checksum = hash_fcn.hexdigest()

        sysmeta = utils.create_system_metadata(self.pid, len(pkg_xml),
                                               checksum, algorithm, RDFXML_FORMATID,
                                               **kwargs)
        flo = StringIO.StringIO(pkg_xml)

        # Save all the objects.
        if self.scimeta and self.scimeta.dirty:
            self._create_or_update(mn_client, cn_client, self.scimeta)
        for scidata_pid in self.scidata_dict.keys():
            scidata = self.scidata_dict.get(scidata_pid)
            if scidata and scidata.dirty:
                self._create_or_update(mn_client, cn_client, scidata)

        response = mn_client.create(pid=self.pid, obj=flo, sysmeta=sysmeta)
        if response is None:
            return None
        else:
            self.original_pid = self.pid
            if self.scimeta:
                self.scimeta.dirty = False
            if self.scidata_dict:
                for scidata in self.scidata_dict.values():
                    scidata.dirty = False
            return response.value()


    def _create_or_update(self, mn_client, cn_client, data_object):
        ''' Either update the specified pid if it already exists or create a new one.
        '''
        if not data_object:
            raise Exception('data object cannot be null')
        if not data_object.pid:
            raise Exception('data object must have a pid')
        if not data_object.fname:
            raise Exception('data object must have a file to write')
        if not data_object.meta:
            raise Exception('data object must have system metadata')
        curr_sysmeta = utils.get_sysmeta_by_pid(data_object.pid, True,
                                                cn_client, mn_client)
        # Create
        if not curr_sysmeta:
            with open(utils.expand_path(data_object.fname), 'r') as f:
                try:
                    return mn_client.create(data_object.pid, f, data_object.meta)
                except DataONEException as e:
                    raise Exception('Unable to create Science Object on Member Node\n{0}'
                                  .format(e.friendly_format()))
        # Update
        else:
            data_object.meta.serialVersion = (curr_sysmeta.serialVersion + 1)
            with open(utils.expand_path(data_object.fname), 'r') as f:
                try:
                    return mn_client.update(data_object.pid, f, data_object.pid, data_object.meta)
                except DataONEException as e:
                    raise Exception('Unable to update Science Object on Member Node\n{0}'
                                  .format(e.friendly_format()))
        # Nothing good happened.
        return None


    def scimeta_add(self, pid, file_name=None, format_id=None, **kwargs):
        ''' Add a scimeta object.
        '''
        if not pid:
            raise Exception('Missing the pid')
        # if (self.scimeta and not
        #                       cli_util.confirm('Do you wish to delete the existing science metadata object?')):
        #   return
        else:
            self.scimeta = None

        if not file_name:
            new_meta = utils.get_sysmeta_by_pid(pid, True)
            if not new_meta:
                raise Exception('Couldn\'t find scimeta in DataONE, and there was no file specified.')
            if not self._is_metadata_format(new_meta.formatId):
                raise Exception('"%s" is not an allowable science metadata type.' % new_meta.formatId)
            new_pid = new_meta.identifier.value()
            if new_pid != pid:
                pid = new_pid

            self.scimeta = self._get_by_pid(pid, new_meta)
            authMN = new_meta.authoritativeMemberNode
            if authMN:
                baseURL = utils.get_baseUrl(authMN.value())
                if baseURL:
                    self.scimeta.url = utils.create_get_url_for_pid(baseURL, pid)

        else:
            complex_path = utils.create_complex_path(file_name)
            if not os.path.exists(complex_path.path):
                raise Exception('%s: file not found' % complex_path.path)
            if not format_id:
                format_id = complex_path.formatId
            if not format_id and configuration.check("format"):
                format_id = configuration.format
            if not format_id:
                raise Exception('The object format could not be determined and was not defined.')
            if not self._is_metadata_format(format_id):
                raise Exception('"%s" is not an allowable science metadata type.' % new_meta.formatId)
                return
            #
            sysmeta = utils.create_sysmeta_from_path(pid, complex_path.path,
                                                     format_id=format_id, **kwargs)
            self.scimeta = DataObject(pid, True, complex_path.path, None, sysmeta,
                                      format_id)
        scidata_list = self._find_scidata(self.scimeta)
        if scidata_list:
            for scidata in scidata_list:
                self.scidata_add(scidata.pid, scidata.fname)


    def scimeta_del(self):
        ''' Remove the science metadata object.
        '''
        # if cli_util.confirm('Are you sure you want to remove the science meta object?'):
        #     self.scimeta = None
        self.scimeta = None



    def scidata_add(self, pid, file_name=None, format_id=None, **kwargs):
        ''' Add a science data object to the list.
        '''
        if not pid:
            raise Exception('Missing the pid')

        if file_name:
            # if utils.get_sysmeta_by_pid(pid):
            #   if not cli_util.confirm('That pid (%s) already exists in DataONE.  Continue?' % pid):
            #     return
            #   raise Exception("The identifer (%s) already exists in DataONE." % pid)

            complex_path = utils.create_complex_path(file_name)
            if not format_id:
                format_id = complex_path.formatId
            if not format_id and configuration.check("format"):
                format_id = configuration.format
            if not format_id:
                raise Exception('The object format could not be determined and was not defined.')
            meta = utils.create_sysmeta_from_path(pid, complex_path.path,
                                                  format_id=format_id, **kwargs)
            self.scidata_dict[pid] = DataObject(pid, True, complex_path.path, 
                                                None, meta, format_id)

        else:
            sysmeta = utils.get_sysmeta_by_pid(pid, True)
            if not sysmeta:
                raise Exception('The identifier (%s) was not found in DataONE.' % pid)
            else:
                pid = sysmeta.identifier.value()
                # if pid in self.scidata_dict:
                #   if not cli_util.confirm('That science data object (%s) is already in the package.  Replace?' % pid):
                #     return
                # Get the scidata object.
                scidata = self._get_by_pid(pid, sysmeta)
                authMN = sysmeta.authoritativeMemberNode
                if authMN:
                    baseURL = utils.get_baseUrl(authMN.value())
                    if baseURL:
                        scidata.url = utils.create_get_url_for_pid(baseURL, pid)
                self.scidata_dict[pid] = scidata


    def scidata_get(self, pid):
        ''' Get the specified scidata object.
        '''
        if pid and pid in self.scidata_dict:
            return self.scidata_dict
        else:
            return None


    def scidata_del(self, pid):
        ''' Remove a science data object.
        '''
        if pid is None:
            raise Exception('Missing the pid')
        if pid in self.scidata_dict:
            # if cli_util.confirm('Are you sure you want to remove the science data object "%s"?' % pid):
            del self.scidata_dict[pid]


    def scidata_clear(self):
        ''' Remove all science data objects
        '''
        if self.scidata_dict is None:
            self.scidata_dict = {}
        elif ((len(self.scidata_dict) > 0)):
            # and cli_util.confirm('Are you sure you want to remove all the science data objects?')):
            self.scidata_dict.clear()


    #== Helpers ===============================================================

    def _get_by_pid(self, pid, sysmeta=None, mn_client=None, cn_client=None):
        ''' Return DataObject
        '''
        if pid is None:
            raise Exception('Missing pid')

        fname = utils.get_object_by_pid(pid, resolve=True, mn_client=mn_client,
                                        cn_client=cn_client)
        if fname:
            meta = sysmeta
            if not meta:
                meta = utils.get_sysmeta_by_pid(pid, True)
            url = utils.create_get_url_for_pid(mn_client.base_url, pid)
            return DataObject(pid, False, fname, url, meta, meta.formatId, None)
        else:
            return None


    def _is_metadata_format(self, formatId):
        ''' Check to see if this formatId specifies a resource map.
        '''
        if formatId is None:
            return False
        elif ((len(formatId) >= 4) and (formatId[:4] == "eml:")):
            return True
        elif ((len(formatId) >= 9) and (formatId[:9] == "FGDC-STD-")):
            return True
        else:
            return False


    def _generate_resmap(self, mn_client_base_url):
        ''' Create a package.
        '''
        # Create the aggregation
        foresite.utils.namespaces['cito'] = Namespace("http://purl.org/spar/cito/")
        aggr = foresite.Aggregation(self.pid)
        aggr._dcterms.title = 'Simple aggregation of science metadata and data.'

        # Create a reference to the science metadata
        uri_scimeta = URIRef(self.scimeta.url)
        res_scimeta = foresite.AggregatedResource(uri_scimeta)
        res_scimeta._dcterms.identifier = self.scimeta.pid
        res_scimeta._dcterms.description = 'Science metadata object.'

        # Create references to the science data
        resource_list = []
        for scidata in self.scidata_dict.values():
            uri_scidata = URIRef(scidata.url)
            res_scidata = foresite.AggregatedResource(uri_scidata)
            res_scidata._dcterms.identifier = scidata.pid
            res_scidata._dcterms.description = 'Science data object'
            res_scidata._cito.isDocumentedBy = uri_scimeta
            res_scimeta._cito.documents = uri_scidata
            resource_list.append(res_scidata)

        # Add all the resources.
        aggr.add_resource(res_scimeta)
        for resource in resource_list:
            aggr.add_resource(resource)

        # Create the resource map
        resmap_url = utils.create_get_url_for_pid(mn_client_base_url,
                                                  format(self.pid))
        self.resmap = foresite.ResourceMap(resmap_url)
        self.resmap._dcterms.identifier = self.pid
        self.resmap.set_aggregation(aggr)
        return self.resmap


    def _serialize(self, fmt='xml', mn_client=None):
        assert(fmt in ALLOWABLE_PACKAGE_SERIALIZATIONS)
        if mn_client is None:
            mn_client = utils.get_d1_mn_client()
        if not self._prepare_urls(mn_client):
            return
        self._generate_resmap(mn_client.base_url)
        if self.resmap.serializer is not None:
            self.resmap.serializer = None
        serializer = foresite.RdfLibSerializer(fmt)
        self.resmap.register_serialization(serializer)
        doc = self.resmap.get_serialization()
        return doc.data


    def _prepare_urls(self, mn_client=None):
        ''' Walk through the objects make sure that everything can be
            serialized.
        '''
        if mn_client is None:
            mn_client = utils.get_d1_mn_client()

        if self.scimeta and not self.scimeta.url:
            if not self._check_item(self.scimeta):
                return False
            elif not self.scimeta.url:
                self.scimeta.url = utils.create_resolve_url_for_pid(mn_client.base_url,
                                                                    self.scimeta.pid)
        if self.scidata_dict:
            for scidata in self.scidata_dict.values():
                if not self._check_item(scidata):
                    return False
                elif not scidata.url:
                    scidata.url = utils.create_resolve_url_for_pid(mn_client.base_url,
                                                                   scidata.pid)
        return True


    def _check_item(self, item):
        errors = []
        if not self.scimeta.pid:
            errors.append('missing pid')
        if not self.scimeta.fname:
            errors.append('missing fname')
        if not self.scimeta.format_id:
            errors.append('missing format-id')
        if len(errors) == 0:
            return True
        else:
            msg = 'Cannot serialize the science object: '
            msg +=  ', '.join(errors)
            raise Exception(msg)


    def _create_object(self, item, mn_client=None, **kwargs):
        ''' Create an object in DataONE. '''
        path = utils.expand_path(item.fname)
        # cli_util.assert_file_exists(path)
        assert(os.path.exists(path))
        if 'format_id' in kwargs:
            del kwargs['format_id']
        sysmeta = utils.create_sysmeta_from_path(item.pid, path,
                                                 format_id=item.format_id,
                                                 **kwargs)
        if mn_client is None:
            mn_client = utils.get_d1_mn_client()
        with open(path, 'r') as f:
            try:
                result = mn_client.create(item.pid, f, sysmeta)
                print 'Created object "%s"' % item.pid
                return result
            except DataONEException as e:
                raise Exception('Unable to create Science Object on Member Node\n{0}'
                              .format(e.friendly_format()))


    def _download_object(self, data_object):
        ''' Download the object. '''
        if not data_object.pid:
            raise Exception('There is no pid specified')
        return self._get_by_pid(data_object.pid, data_object.meta)


    def _find_scidata(self, scimeta):
        '''  Search through an eml://ecoinformatics.org/eml-2.x.x document '''
        '''  looking for science data objects.                             '''
        '''                                                                '''
        '''               THIS IS GOING TO BE DIFFICULT!                   '''
        return ()


class DataObject(object):

    def __init__(self, pid=None, dirty=None, fname=None, url=None, meta=None,
                 format_id=None, documented_by=None):
        ''' Create a data object
        '''
        self.pid = pid
        self.dirty = dirty
        self.fname = fname
        self.url = url
        self.meta = meta
        self.format_id = format_id
        self.documented_by = documented_by

    def is_dirty(self):
        return (self.dirty is not None) and self.dirty

    def str(self): #@ReservedAssignment
        m = 'None'
        if self.meta is not None:
            m = '<...>'
        return 'DataObject[pid=%s,dirty=%s,fname=%s,meta=%s]' % \
            (self.pid, str(self.dirty), self.fname, m)

    def from_url(self, url):
        self.url = url;
        ndx = url.find('/resolve/') + 8
        if ndx > 8:
            self.pid = url[ndx:]


    def summary(self, prefix, pretty, verbose):
        p = prefix
        if not prefix:
            p = '  '

        if (verbose is not None) and verbose:
            pass
        else:
            flags = ''
            pre = ' ('
            post = ''

            if (self.dirty is not None) and self.dirty:
                flags += pre + 'needs saving'
                pre = ', '
                post = ')'
            if self.fname is not None:
                flags += pre + 'has an object file'
                if verbose:
                    flags += ' (%s)' % self.fname
                pre = ', '
                post = ')'
            if self.meta is not None:
                flags += pre + 'has sysmeta'
                pre = ', '
                post = ')'

            flags = flags + post
            print '%s%s%s' % (p, self.pid, flags)


def run_pkg_test():
    configuration.mn_url = "https://mn-demo-9.test.dataone.org/knb/d1/mn"
    configuration.cn_url = "https://cn-stage-2.test.dataone.org/cn"
    cn_client = utils.get_d1_cn_client()
    mn_client = utils.get_d1_mn_client()

    pid = "dakoop_test_pkg603"
    meta_pid = "dakoop_cadwsap003"
    meta_fname = \
        "/vistrails/local_packages/dataone/package_data/cadwsap-s2910001-001.xml"
    meta_format = "FGDC-STD-001-1998"
    data_list = [("dakoop_cadwsap_main003", "/vistrails/local_packages/dataone/package_data/cadwsap-s2910001-001-main.csv", "text/csv"),
                 ("dakoop_cadwsap_vuln003", "/vistrails/local_packages/dataone/package_data/cadwsap-s2910001-001-vuln.csv", "text/csv")]

    pkg = DataPackage(pid)
    sysmeta_kwargs = {"format_id": meta_format,
                      "submitter": "dakoop",
                      "owner": "dakoop",
                      "orig_mn": configuration.mn_url,
                      "auth_mn": configuration.mn_url}
    pkg.scimeta_add(meta_pid, meta_fname, **sysmeta_kwargs)
    for (data_pid, data_fname, data_format) in data_list:
        sysmeta_kwargs["format_id"] = data_format
        pkg.scidata_add(data_pid, data_fname, **sysmeta_kwargs)
    del sysmeta_kwargs["format_id"]
    pkg.save(mn_client, cn_client, **sysmeta_kwargs)

if __name__ == '__main__':
    run_pkg_test()
