###############################################################################
## VisTrails wrapper for DataONE
## By David Koop, dkoop@poly.edu
##
## Copyright (C) 2012, NYU-Poly.
###############################################################################

import datetime
import os
import shutil
from StringIO import StringIO
import urllib

from core.modules.basic_modules import String, new_constant
from core.modules.vistrails_module import Module, ModuleError

import d1_client
import d1_common
import d1_common.types.generated.dataoneTypes as dataoneTypes
from d1_client.mnclient import MemberNodeClient
from d1_client.cnclient import CoordinatingNodeClient

from access_control import access_control
from replication_policy import replication_policy
from data_package import DataPackage
import identifiers
import utils


D1DateTime = new_constant("D1DateTime", staticmethod(str), "", 
                          staticmethod(lambda x: type(x) == str),
                          base_class=String)
D1Identifier = new_constant("D1Identifier", staticmethod(str), "", 
                            staticmethod(lambda x: type(x) == str),
                            base_class=String)
D1Identifier._input_ports = [('value', String)]

def get_cn_url(module, required=True):
    if module.hasInputFromPort("coordinatingNodeURL"):
        return module.getInputFromPort("coordinatingNodeURL")
    elif configuration.check("cn_url"):
        return configuration.cn_url
    elif required:
        raise ModuleError(module, "The Coordinating Node URL is required. " \
                              "Please set it globally in the package " \
                              "configuration or as a parameter.")
    return None

def get_mn_url(module, required=True):
    if module.hasInputFromPort("memberNodeURL"):
        return module.getInputFromPort("memberNodeURL")
    elif configuration.check("mn_url"):
        return configuration.mn_url
    elif required:
        raise ModuleError(module, "The Member Node URL is required. " \
                              "Please set it globally in the package " \
                              "configuration or as a parameter.")
    return None

class D1Authentication(Module):
    _input_ports = [("keyFile", "(edu.utah.sci.vistrails.basic:File)"),
                    ("certFile", "(edu.utah.sci.vistrails.basic:File)"),
                    ("anonymous", "(edu.utah.sci.vistrails.basic:Boolean)")]
    _output_ports = [("self", "(%s:D1Authentication)" % identifiers.identifier)]
    
    def __init__(self):
        Module.__init__(self)
        self.key_file = default_key_file
        self.cert_file = default_cert_file
        self.anonymous = default_anonymous

    def compute(self):
        if self.hasInputFromPort("keyFile"):
            self.key_file = self.getInputFromPort("keyFile").name
        if self.hasInputFromPort("certFile"):
            self.cert_file = self.getInputFromPort("certFile").name
        if self.hasInputFromPort("anonymous"):
            self.anonymous = self.getInputFromPort("anonymous")

class D1AccessPolicy(Module):
    _input_ports = [("addSubjectPermissions", 
                     "(edu.utah.sci.vistrails.basic:String,"
                     "edu.utah.sci.vistrails.basic:String)"),
                    ("public", "(edu.utah.sci.vistrails.basic:Boolean)",
                     {"defaults": ["True"]})]
    _output_ports = [("self", "(%s:D1AccessPolicy)" % \
                          identifiers.identifier)]
    
    def __init__(self):
        Module.__init__(self)
        self.access_control = access_control()
        
    def compute(self):
        for (subject, permission) in \
                self.forceGetInputListFromPort("addSubjectPermissions"):
            self.access_control.add_allowed_subject(subject, permission)
        if self.hasInputFromPort("public"):
            self.allow_public(self.getInputFromPort("public"))

class D1ReplicationPolicy(Module):
    _input_ports = [("addPreferred", "(edu.utah.sci.vistrails.basic:String)"),
                    ("addBlocked", "(edu.utah.sci.vistrails.basic:String)"),
                    ("numberOfReplicas", 
                     "(edu.utah.sci.vistrails.basic:Integer)"),
                    ("replicationAllowed",
                     "(edu.utah.sci.vistrails.basic:Boolean)",
                     {"defaults": ["True"]})
                    ]
    _output_ports = [("self", "(%s:D1ReplicationPolicy)" % \
                          identifiers.identifier)]

    def __init__(self):
        Module.__init__(self)
        self.replication_policy = replication_policy()

    def compute(self):
        for mn_url in self.forceGetInputListFromPort("addPreferred"):
            self.replication_policy.add_preferred(mn_url)
        for mn_url in self.forceGetInputListFromPort("addBlocked"):
            self.replication_policy.add_blocked(mn_url)
        if self.hasInputFromPort("numberOfReplicas"):
            self.replication_policy.set_number_of_replicas(
                self.getInputFromPort("numberOfReplicas"))
        if self.hasInputFromPort("replicatedAllowed"):
            self.replication_policy.set_replication_allowed(
                self.getInputFromPort("replicationAllowed"))

class D1SystemMetadata(Module):
    _input_ports = [("accessPolicy", "(%s:D1AccessPolicy)" % \
                         identifiers.identifier),
                    ("replicationPolicy", "(%s:D1ReplicationPolicy)" % \
                         identifiers.identifier),
                    ("format", "(edu.utah.sci.vistrails.basic:String)"),
                    ("submitter", "(edu.utah.sci.vistrails.basic:String)"),
                    ("owner", "(edu.utah.sci.vistrails.basic:String)"),
                    ("originMN", "(edu.utah.sci.vistrails.basic:String)"),
                    ("authMN", "(edu.utah.sci.vistrails.basic:String)"),
                    ("checksum", "(edu.utah.sci.vistrails.basic:String)"),
                    ]

    _output_ports = [("self", "(%s:D1SystemMetadata)" % \
                          identifiers.identifier)]    

    def __init__(self, access_policy=None, replication_policy=None,
                 format=None, submitter=None, owner=None, origin_mn=None,
                 auth_mn=None, checksum="SHA-1"):
        Module.__init__(self)
        self.access_policy = access_policy
        self.replication_policy = replication_policy
        self.format = format
        self.submitter = submitter
        self.owner = owner
        self.origin_mn = origin_mn
        self.auth_mn = auth_mn
        self.checksum = checksum

    def compute(self):
        if self.hasInputFromPort("accessPolicy"):
            self.access_policy = self.getInputFromPort("accessPolicy")
        if self.hasInputFromPort("replicationPolicy"):
            self.replication_policy = self.getInputFromPort("replicationPolicy")
        if self.hasInputFromPort("format"):
            self.format = self.getInputFromPort("format")
        if self.hasInputFromPort("submitter"):
            self.submitter = self.getInputFromPort("submitter")
        if self.hasInputFromPort("owner"):
            self.owner = self.getInputFromPort("owner")
        if self.hasInputFromPort("originMN"):
            self.origin_mn = self.getInputFromPort("originMN")
        if self.hasInputFromPort("authMN"):
            self.auth_mn = self.getInputFromPort("authMN")
        if self.hasInputFromPort("checksum"):
            self.checksum = self.getInputFromPort("checksum")

    def to_dict(self):
        return {"access_policy": self.access_policy,
                "replication_policy": self.replication_policy,
                "format_id": self.format,
                "submitter": self.submitter,
                "owner": self.owner,
                "origin_mn": self.origin_mn,
                "auth_mn": self.auth_mn,
                "algorithm": self.checksum}

class D1GetObject(Module):
    _input_ports = [("identifier", "(%s:D1Identifier)" % identifiers.identifier),
                    ("coordinatingNodeURL", 
                     "(edu.utah.sci.vistrails.basic:String)"),
                    ("memeberNodeURL",
                     "(edu.utah.sci.vistrails.basic:String)")]
    _output_ports = [("file", "(edu.utah.sci.vistrails.basic:File)")]

    def compute(self, get_method):
        """compute dumps to object (either science data or metadata)
        to a file object, get_method is the unbound method
        (e.g. MemberNodeClient.get)"""

        pid = self.getInputFromPort("identifier")

        cn_url = get_cn_url(self)
        mn_url = get_mn_url(self)
        cn_client = utils.get_d1_cn_client(cn_url=cn_url)
        mn_client = utils.get_d1_mn_client(mn_url=mn_url)
        self.annotate({'cn_url': cn_url, 'mn_url': used_mn_url})
        output_file = self.interpreter.filePool.create_file()
        # FIXME would be nice to know which member node the data was
        # downloaded from
        res = get_method(pid, mn_client, cn_client, True, output_file.name)
        if res is None:
            raise ModuleError(self, "Object could not be retrieved")

        # do something with identifier.pid and output_file.name
        self.setResult("file", output_file)

class D1GetMetadata(D1GetObject):
    @staticmethod
    def get_metadata(pid, mn_client, cn_client, full_resolve, output_fname):
        res = utils.get_sysmeta_by_pid(pid, full_resolve, cn_client, mn_client)
        if res is not None:
            utils.write_file_output(StringIO(res.to_xml()), 
                                    output_file.name)
        return output_file.name

    def compute(self):
        # getSystemMetadata returns a pyxb object that can be
        # converted to xml and then dumped to a file so we have an
        # intermediate method here (compare to D1GetData.compute)
        D1GetObject.compute(self, D1GetMetadata.get_metadata)

class D1GetData(D1GetObject):
    @staticmethod
    def get_data(pid, mn_client, cn_client, full_resolve, output_fname):
        return utils.get_object_by_pid(pid, output_fname, full_resolve, 
                                       mn_client, cn_client)
        
    def compute(self):
        D1GetObject.compute(self, D1GetData.get_data)

class D1PutObject(Module):
    _input_ports = [("memberNodeURL", "(edu.utah.sci.vistrails.basic:String)"),
                    ("coordinatingNodeURL", 
                     "(edu.utah.sci.vistrails.basic:String)"),
                    ("authentication", "(%s:D1Authentication)" % \
                         identifiers.identifier),
                    ("systemMetadata", "(%s:D1SystemMetadata)" % \
                         identifiers.identifier),
                    ("updateIfExists", 
                     "(edu.utah.sci.vistrails.basic:Boolean)", True)
                    ]
    
    def create_object(self, pid, mn_client, cn_client):
        raise ModuleError(self, "A subclass of D1PutObject must define " \
                              "the create_object method.")

    def update_object(self, pid, mn_client, cn_client):
        raise ModuleError(self, "A subclass of D1PutObject must define " \
                              "the update_object method")

    def compute(self, pid):
        cert_file = None
        key_file = None
        cn_url = get_cn_url(self)
        mn_url = get_mn_url(self)

        if self.hasInputFromPort("authentication"):
            auth = self.getInputFromPort("authentication")
            cert_file = auth.cert_file
            key_file = auth.key_file

        self.annotate({"cn_url": cn_url, "mn_url": mn_url})

        mn_client = utils.get_d1_mn_client(mn_url=mn_url, cert_file=cert_file, 
                                           key_file=key_file)
        cn_client = utils.get_d1_cn_client(cn_url=cn_url)
        
        # if it already exists
        if utils.get_sysmeta_by_pid(pid, True, cn_client, mn_client):
            if not self.forceGetInputFromPort("updateIfExists", False):
                raise ModuleError(self, 'Cannot add data: ' \
                                      'identifer "%s" already exists.')
            else:
                self.update_object(pid, mn_client, cn_client)
        self.create_object(pid, mn_client, cn_client)

class D1PutData(D1PutObject):
    _input_ports = [("identifier", "(%s:D1Identifier)" % \
                         identifiers.identifier),
                    ("inputFile", "(edu.utah.sci.vistrails.basic:File)")]

    def create_object(self, pid, mn_client, cn_client):
        obj = self.getInputFromPort("inputFile")
        if self.hasInputFromPort("systemMetadata"):
            local_sysmeta = self.getInputFromPort("systemMetadata")
            sysmeta_kwargs = local_sysmeta.to_dict()
        else:
            sysmeta_kwargs = {}
        sysmeta = utils.create_sysmeta_from_path(pid, obj.name, 
                                                 **sysmeta_kwargs)

        f = open(obj.name, 'r')
        retval = mn_client.create(pid, f, sysmeta)
        f.close()

    def update_object(self, pid, mn_client, cn_client):
        raise ModuleError("Update is not implemented yet.")

    def compute(self):
        pid = self.getInputFromPort("identifier")
        D1PutObject.compute(self, pid)

class D1DataObject(Module):
    _input_ports = [("identifier", "(%s:D1Identifier)" % \
                         identifiers.identifier),
                    ("file", "(edu.utah.sci.vistrails.basic:File)"),
                    ("formatId", "(edu.utah.sci.vistrails.basic:String)", 
                     True)]
    _output_ports = [("self", "(%s:D1DataObject)" % identifiers.identifier)]

    def __init__(self):
        Module.__init__(self)
        self.identifier = None
        self.filename = None
        self.format_id = None

    def compute(self):
        self.identifier = self.getInputFromPort("identifier")
        if self.hasInputFromPort("file"):
            self.filename = self.getInputFromPort("file").name
        if self.hasInputFromPort("formatId"):
            self.format_id = self.getInputFromPort("formatId")

class D1Package(Module):
    _input_ports = [("identifier", "(%s:D1Identifier)" % \
                         identifiers.identifier),
                    ("data", "(%s:D1DataObject)" % identifiers.identifier),
                    ("metadata", "(%s:D1DataObject)" % identifiers.identifier)]
    _output_ports = [("self", "(%s:D1Package)" % identifiers.identifier)]

    def __init__(self):
        Module.__init__(self)
        self.meta = None
        self.data_list = []

    def compute(self):
        self.identifier = self.getInputFromPort("identifier")
        self.meta = self.getInputFromPort("metadata")
        self.data_list = self.getInputListFromPort("data")

class D1PutPackage(D1PutObject):
    _input_ports = [("package", "(%s:D1Package)" % \
                         identifiers.identifier),
                    ]

    def create_object(self, pid, mn_client, cn_client):
        local_pkg = self.getInputFromPort("package")
        pkg = DataPackage(pid)

        if self.hasInputFromPort("systemMetadata"):
            local_sysmeta = self.getInputFromPort("systemMetadata")
            sysmeta_kwargs = local_sysmeta.to_dict()
        else:
            sysmeta_kwargs = {}

        sysmeta_kwargs["format_id"] = local_pkg.meta.format_id
        pkg.scimeta_add(local_pkg.meta.identifier, local_pkg.meta.filename,
                        **sysmeta_kwargs)
        for obj in local_pkg.data_list:
            sysmeta_kwargs["format_id"] = obj.format_id
            pkg.scidata_add(obj.identifier, obj.filename, **sysmeta_kwargs)
        del sysmeta_kwargs["format_id"]
        pkg.save(mn_client, cn_client, **sysmeta_kwargs)
        
    def update_object(self, pid, mn_client, cn_client):
        raise ModuleError("Update is not implemented yet.")

    def compute(self):
        local_pkg = self.getInputFromPort("package")
        D1PutObject.compute(self, local_pkg.identifier)

class D1GetPackage(Module):
    _input_ports = [("identifier", "(%s:D1Identifier)" % \
                         identifiers.identifier),
                    ("coordinatingNodeURL", 
                     "(edu.utah.sci.vistrails.basic:String)"),
                    ("authentication", "(%s:D1Authentication)" % \
                         identifiers.identifier)]
    
    # FIXME
    def compute(self):
        raise ModuleError(self, "Not implemented yet.")


# Search Fields parsed from:
# http://mule1.dataone.org/ArchitectureDocs-current/design/SearchMetadata.html

class D1Search(Module):
    _input_ports = [("LTERSite", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "Data provider organization identifier, for sources within the LTER network."}),
("abstract", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "The full text of the abstract as provided in the science metadata document."}),
("author", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "Principle Investigator (PI) / Author as listed in the metadata document."}),
("authorLastName", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "The LAST name(s) of the author(s)"}),
("authoritativeMN", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "The node Id of the authoritative Member Node for the object."}),
("beginDate", "(org.vistrails.dataone:D1DateTime)", True, {"docstring": "The starting date of the temporal range of the content described by the metadata document."}),
("blockedReplicationMN", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "A multi-valued field that contains the node Ids of member nodes that are blocked from holding replicas of this object."}),
("changePermission", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "List of subjects (groups and individuals) that have change permission on PID."}),
("checksum", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "The checksum for the object"}),
("checksumAlgorithm", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "Algorithm used for generating the object checksum"}),
("class", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "Taxonomic class name(s)"}),
("contactOrganization", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "Name of the organization to contact for more information about the dataset"}),
("contactOrganizationText", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "Copy from contactOrganization"}),
("dataUrl", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "The URL that can be used to resolve the location of the object given its PID."}),
("datasource", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "The node Id of the member node that originally contributed the content."}),
("dateModified", "(org.vistrails.dataone:D1DateTime)", True, {"docstring": "The date and time when the object system metadata was last updated."}),
("dateUploaded", "(org.vistrails.dataone:D1DateTime)", True, {"docstring": "The date and time when the object was uploaded to the Member Node."}),
("decade", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "The latest decade that is covered by the dataset, expressed in the form \"1999-2009\""}),
("documents", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "Lists all PIDs that this object describes. Obtained by parsing all resource maps in which this object is referenced. Not set for data or resource map objects."}),
("eastBoundCoord", "(edu.utah.sci.vistrails.basic:Float)", True, {"docstring": "Eastern most longitude of the spatial extent, in decimal degrees, WGS84"}),
("edition", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "The version or edition number of the item described."}),
("endDate", "(org.vistrails.dataone:D1DateTime)", True, {"docstring": "The ending date of the temporal range of the content described by the metadata document."}),
("family", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "Taxonomic family name(s)"}),
("fileID", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "Contains the CNRead.resolve() URL for the object ONLY if the object is a science metadata object."}),
("formatId", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "The format identifier indicating the type of content this record refers to."}),
("fullText", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "Full text of the metadata record, used to support full text searches"}),
("gcmdKeyword", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "Keywords drawn from the GCMD controlled vocabulary"}),
("genus", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "Taxonomic genus name(s)"}),
("geoform", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "The name of the general form in which the item's geospatial data is presented"}),
("id", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "The identifier of the object being indexed."}),
("identifier", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "Copy from id"}),
("investigator", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "Name of the investigator(s) responsible for developing the dataset and associated content."}),
("investigatorText", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "Copy from investigator"}),
("isDocumentedBy", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "Lists all PIDs that describe this object. Obtained by parsing all resource maps in which this object is referenced."}),
("isPublic", "(edu.utah.sci.vistrails.basic:Boolean)", True, {"docstring": "Set to True if the DataONE public user is present in the list of subjects with readPermission on PID."}),
("isSpatial", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "Set to \"Y\" for records that contain spatial information"}),
("keyConcept", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "Terms drawn from a controlled vocabulary of concepts that are applicable to the content described by the metadata document."}),
("keywords", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "Keywords recorded in the science metadata document. These may be controlled by the generator of the metadata or by the metadata standard of the document, but are effectively uncontrolled within the DataONE context."}),
("keywordsText", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "Copy from keywords"}),
("kingdom", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "Taxonomic kingdom(s)"}),
("namedLocation", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "The name of the location(s) relevant to the content described by the metadata document."}),
("noBoundingBox", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "Set to \"Y\" if there is no bounding box information available (i.e., the east, west, north, south most coordinates)"}),
("northBoundCoord", "(edu.utah.sci.vistrails.basic:Float)", True, {"docstring": "Northern most latitude of the spatial extent, in decimal degrees, WGS84"}),
("numberReplicas", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "Requested number of replicas for the object"}),
("obsoletes", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "If set, indicates the object that this record obsoletes."}),
("ogcUrl", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "URL for Open Geospatial Web service if available."}),
("order", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "Taxonomic order name(s)"}),
("origin", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "Investigator or Investigator organization name."}),
("originText", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "Copy from origin"}),
("originator", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "Investigator or Investigator organization name. Derived by normalizing origin."}),
("originatorText", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "Copy from originator"}),
("parameter", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "A characteristic, or variable, that is measured or derived as part of data-collection activities."}),
("parameterText", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "Copy from parameter"}),
("phylum", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "Taxonomic phylum (or division) name(s)"}),
("placeKey", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "A place name keyword, assigned by the metadata creator. It is one keyword from the thesaurus named in <placekt>"}),
("preferredReplicationMN", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "A list of member node identifiers that are preferred replication targets for this object."}),
("presentationCat", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "Type of data being preserved (maps, text, etc.)"}),
("project", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "The authorized name of a research effort for which data is collected. This name is often reduced to a convenient abbreviation or acronym. All investigators involved in a project should use a common, agreed-upon name."}),
("projectText", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "Copy from project"}),
("pubDate", "(org.vistrails.dataone:D1DateTime)", True, {"docstring": "Publication date for the dataset (this may or may not be coincident with when the content is added to DataONE)."}),
("purpose", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "The \"Purpose\" describes the \"why\" aspects of the data set (For example, why was the data set created?)."}),
("readPermission", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "List of subjects (groups and individuals) that have read permission on PID."}),
("relatedOrganizations", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "Organizations that played an indirect role in the development of the data set and metadata that should be cited or mentioned as contributing to the development of the data or metadata."}),
("replicaMN", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "One or more node Ids holding copies of the object."}),
("replicationAllowed", "(edu.utah.sci.vistrails.basic:Boolean)", True, {"docstring": "True if this object can be replicated."}),
("resourceMap", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "List of resource map PIDs that reference this PID."}),
("rightsHolder", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "The Subject that acts as the rights holder for the object."}),
("scientificName", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "Taxonomic scientific name(s) at the most precise level available for the organisms of relevance to the dataset"}),
("sensor", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "Also called \"instrument.\" A device that is used for collecting data for a data set."}),
("sensorText", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "Copy from sensor"}),
("site", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "The name or description of the physical location where the data were collected"}),
("siteText", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "Copy from site"}),
("size", "(edu.utah.sci.vistrails.basic:Integer)", True, {"docstring": "The size of the object, in bytes."}),
("sku", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "Copy from id"}),
("source", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "Also called \"platform.\" The mechanism used to support the sensor or instrument that gathers data"}),
("sourceText", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "Copy from source"}),
("southBoundCoord", "(edu.utah.sci.vistrails.basic:Float)", True, {"docstring": "Southern most latitude of the spatial extent, in decimal degrees, WGS84"}),
("species", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "Taxonomic species name(s)"}),
("submitter", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "The Subject name of the original submitter of the content to DataONE."}),
("term", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "A secondary subject area within which parameters can be categorized. Approved terms include \"agricultural chemicals\" and \"atmospheric chemistry,\" among many others. When entering a term in the LandVal Metadata Editor, users should select a standard expression from the pick list for terms if at all possible."}),
("termText", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "Copy from term"}),
("text", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "contactOrganization, datasource, decade, features, fileID, fullText, gcmdKeyword, geoform, id, includes, investigator, keywords, LTERSite, manu, name, origin, originator, parameter, placeKey, presentationCat, project, purpose, sensor, site, source, term, title, topic"}),
("title", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "Title of the dataset as recorded in the science metadata."}),
("titleText", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "Copy from title"}),
("titlestr", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "Copy from title"}),
("topic", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "The most general subject area within which a parameter is categorized. Approved topics include \"agriculture,\" \"atmosphere,\" and \"hydrosphere,\" among others."}),
("topicText", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "Copy from topic"}),
("updateDate", "(org.vistrails.dataone:D1DateTime)", True, {"docstring": "Copy from dateUploaded"}),
("webUrl", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "Link to the investigator's web-site."}),
("westBoundCoord", "(edu.utah.sci.vistrails.basic:Float)", True, {"docstring": "Western most longitude of the spatial extent, in decimal degrees, WGS84"}),
("writePermission", "(edu.utah.sci.vistrails.basic:String)", True, {"docstring": "List of subjects (groups and individuals) that have write permission on PID."})]
    _output_ports = [('dataIdentifierList', 
                      "(edu.utah.sci.vistrails.basic:List)")]
    
    def compute(self):
        dataIdentifierList = []
        setResult("dataIdentifierList", dataIdentifierList)

_modules = [D1DateTime, D1Identifier, 
            # D1Search, 
            (D1GetObject, {"abstract": True}),
            (D1PutObject, {"abstract": True}),
            D1GetData, D1GetMetadata,
            D1Authentication, D1AccessPolicy, D1ReplicationPolicy, 
            D1SystemMetadata, 
            D1PutData, D1DataObject, D1Package, D1PutPackage]


def initialize():
    pass
