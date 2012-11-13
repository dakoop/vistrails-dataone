from vistrails.core.configuration import ConfigurationObject
from identifiers import *

configuration = ConfigurationObject(cn_url=(None, str),
                                    mn_url=(None, str),
                                    cert_file=(None, str),
                                    key_file=(None, str),
                                    anonymous=True,
                                    )

