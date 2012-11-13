###############################################################################
## VisTrails wrapper for DataONE
## By David Koop, dkoop@poly.edu
##
## Copyright (C) 2012, NYU-Poly.
###############################################################################

import datetime
import os
import shutil
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
import identifiers

my_d1_mn_client = None
my_d1_cn_client = None
default_cn_url = None
default_mn_url = None
default_cert_file = None
default_key_file = None
default_anonymous = True

def create_d1_mn_client(mn_url=None, cert_file=None, key_file=None):
    global my_d1_mn_client
    if mn_url is None:
        mn_url = default_mn_url
    if cert_file is None:
        cert_file = default_cert_file
        if key_file is None:
            key_file = default_key_file
    elif key_file is None:
        key_file = cert_file
    my_d1_mn_client = MemberNodeClient(mn_url, cert_path=cert_file, 
                                       key_path=key_file)
    return my_d1_mn_client
    
def get_d1_mn_client(*args, **kwargs):
    return create_d1_mn_client(*args, **kwargs)
    # if my_d1_client is None:
    #     return create_d1_client()
    # return my_d1_client

def create_d1_cn_client(cn_url=None):
    global my_d1_cn_client
    my_d1_cn_client = CoordinatingNodeClient(cn_url)
    return my_d1_cn_client

def get_d1_cn_client(*args, **kwargs):
    return create_d1_cn_client(*args, **kwargs)


# Search Fields parsed from:
# http://mule1.dataone.org/ArchitectureDocs-current/design/SearchMetadata.html

D1DateTime = new_constant("D1DateTime", staticmethod(str), "", 
                          staticmethod(lambda x: type(x) == str),
                          base_class=String)
D1Identifier = new_constant("D1Identifier", staticmethod(str), "", 
                            staticmethod(lambda x: type(x) == str),
                            base_class=String)

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

class D1GetData(Module):
    _input_ports = [("identifier", "(%s:D1Identifier)" % identifiers.identifier)]
    _output_ports = [("file", "(edu.utah.sci.vistrails.basic:File)")]
    
    def compute(self):
        pid = self.getInputFromPort("identifier")

        cn_client = get_d1_cn_client()
        result = cn_client.resolve(pid)
        file_like_object = None
        for mn_url in result.objectLocation:
            mn_client = get_d1_mn_client(mn_url)
            try:
                file_like_object = mn_client.get(pid)
            except Exception:
                pass
        if file_like_object is None:
            raise ModuleError(self, "Object could not be retrieved")
            
        # encoded_pid = urllib.quote_plus(identifier)
        # file_like_object = client.get(encoded_pid)
        print "identifier:", identifier
        try:
            output_file = self.interpreter.filePool.create_file()
            output_f = open(output_file.name, 'wb')
            shutil.copyfileobj(file_like_object, output_f)
            output_f.close()
        except EnvironmentError as (errno, strerror):
            error_message_lines = []
            error_message_lines.append(
                'Could not write to object_file: {0}'.format(path))
            error_message_lines.append(
                'I/O error({0}): {1}'.format(errno, strerror))
            error_message = '\n'.join(error_message_lines)
            raise ModuleError(self, error_message)

        # do something with identifier.pid and output_file.name
        self.setResult("file", output_file)

class D1GetMetadata(Module):
    _input_ports = [("identifier", "(%s:D1Identifier)" % identifiers.identifier)]
    _output_ports = [("file", "(edu.utah.sci.vistrails.basic:File)")]
    
    def compute(self):
        identifier = set.getInputFromPort("identifier")
        output_file = self.interpreter.filePool.create_file()

        # do something with identifier.pid and output_file.name
        self.setResult("file", output_file)    

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

class D1PutData(Module):
    _input_ports = [("inputFile", "(edu.utah.sci.vistrails.basic:File)"),
                    ("identifier", "(%s:D1Identifier)" % \
                         identifiers.identifier),
                    ("memberNodeURL", "(edu.utah.sci.vistrails.basic:String)"),
                    ("authentication", "(%s:D1Authentication)" % \
                         identifiers.identifier),
                    ("systemMetadata", "(%s:D1SystemMetadata)" % \
                         identifiers.identifier),
                    ]
    _output_ports = []

    """get_checksum copied from DataONECLI"""
    def get_checksum(self, path, algorithm='SHA-1', block_size=1024 * 1024):
        h = d1_common.util.get_checksum_calculator_by_dataone_designator(
            algorithm)
        with open(path, 'r') as f:
          while True:
            data = f.read(block_size)
            if not data:
                break
            h.update(data)
        return h.hexdigest()

    """get_file_size copied from DataONECLI"""
    def get_file_size(self, path):
        with open(path, 'r') as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
        return size

    def compute(self):
        pid = self.getInputFromPort("identifier")
        obj = self.getInputFromPort("inputFile")

        mn_url = None
        cert_file = None
        key_file = None
        if self.hasInputFromPort("memberNodeURL"):
            mn_url = self.getInputFromPort("memberNodeURL")
        if self.hasInputFromPort("authentication"):
            auth = self.getInputFromPort("authentication")
            cert_file = auth.cert_file
            key_file = auth.key_file
        mn_client = get_d1_mn_client(mn_url=mn_url, cert_file=cert_file, 
                                     key_file=key_file)

        # FIXME need to determine what meta needs to look like
        local_sysmeta = self.getInputFromPort("systemMetadata")
        print "local_sysmeta:", local_sysmeta
        checksum = self.get_checksum(obj.name, local_sysmeta.checksum)
        file_size = self.get_file_size(obj.name)
        
        sysmeta = dataoneTypes.systemMetadata()
        sysmeta.serialVersion = 1
        sysmeta.identifier = pid
        if local_sysmeta.format is None:
            sysmeta.formatId = 'text/csv'
        else:
            sysmeta.formatId = local_sysmeta.format
        sysmeta.size = file_size
        if local_sysmeta.submitter is None:
            sysmeta.submitter = "DAKOOP_TEST"
        else:
            sysmeta.submitter = local_sysmeta.submitter
        if local_sysmeta.owner is None:
            sysmeta.rightsHolder = "DAKOOP_TEST"
        else:
            sysmeta.rightsHolder = local_sysmeta.owner
        sysmeta.checksum = dataoneTypes.checksum(checksum)
        sysmeta.checksum.algorithm = local_sysmeta.checksum
        sysmeta.dateUploaded = datetime.datetime.utcnow()
        sysmeta.dateSysMetadataModified = datetime.datetime.utcnow()
        if local_sysmeta.origin_mn is None:
            sysmeta.originmn = mn_client.base_url
        else:
            sysmeta.originmn = local_sysmeta.origin_mn
        if local_sysmeta.auth_mn is None:
            sysmeta.authoritativemn = mn_client.base_url
        else:
            sysmeta.authoritativemn = local_sysmeta.auth_mn
        if local_sysmeta.access_policy is None:
            sysmeta.accessPolicy = access_control().to_pyxb()
        else:
            sysmeta.accessPolicy = local_sysmeta.access_policy.to_pyxb()
        if local_sysmeta.replication_policy is None:
            sysmeta.replicationPolicy = replication_policy().to_pyxb()
        else:
            sysmeta.replicationPolicy = \
                local_sysmeta.replication_policy.to_pyxb()
        
        f = open(obj.name, 'r')
        retval = mn_client.create(pid, f, sysmeta)
        print "RETVAL:", retval

_modules = [D1DateTime, D1Identifier, 
            D1Search, D1GetData, D1GetMetadata,
            D1Authentication, D1AccessPolicy, D1ReplicationPolicy, 
            D1SystemMetadata, D1PutData]


def initialize():
    global default_cn_url, default_mn_url, default_cert_file, default_key_file
    
    if configuration.check('cn_url'):
        default_cn_url = configuration.cn_url
    if configuration.check('mn_url'):
        default_mn_url = configuration.mn_url
    if configuration.check('cert_file'):
        default_cert_file = configuration.cert_file
    if configuration.check('key_file'):
        default_key_file = configuration.key_file
    if configuration.check('anonymous'):
        default_anonymous = configuration.anonymous
