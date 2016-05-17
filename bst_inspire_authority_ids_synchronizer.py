"""BibAuthority INSPIRE ids synchronizer (Invenio Bibliographic Tasklet).

Synchronize authority records of the "CERN People" collection on CDS with
INSPIRE authority ids. INSPIRE-HEP (inspirehep.net) provides a monthly updated
dump of their records. Thanks!

Usage:
    $ bibtasklet \
        -N bibauthority-people \
        -T bst_inspire_authority_ids_synchronizer \
        [-a url [default: "http://inspirehep.net/dumps/HepNames-records.xml.gz"]
            tmp_dir [default: invenio.config.TMPDIR]]
"""

import gzip
import os
import os.path
import sys
import urllib

from lxml import etree

from invenio.bibtask import task_low_level_submission, write_message
from invenio.config import CFG_TMPDIR
from invenio.search_engine import perform_request_search
from invenio.search_engine_utils import get_fieldvalues


SYNC_URL_INSPIRE_RECORDS_SRC = (
    "http://inspirehep.net/dumps/HepNames-records.xml.gz")
SYNC_LOCAL_TMP_DIR = CFG_TMPDIR
SYNC_LOCAL_INSPIRE_RECORDS_FILE_NAME = "HepNames-records.xml.gz"
SYNC_LOCAL_CDS_RECORDS_UPDATES_FILE_NAME = "cds-records-updates.xml"


# TODO: remove in production
def write_message(msg, stream=None):
    """Replaces invenio.bibtask.write_message for test purposes."""
    print msg


def get_record_ids():
    """Return a list of record ids for 'CERN People' collection."""
    return perform_request_search(cc="CERN People")


def get_inspire_dump(src, dest_gz_tmp):
    """Return file content for a given gzipped file.

    'src' will be copied to 'dest_gz_tmp' representing the local file path. The
    copy will be removed automatically once 'src' has been unzipped and its
    content read.

    :param string src: valid URL to the gzip (.gz) file
        Example:
            "http://inspirehep.net/dumps/HepNames-records.xml.gz"
    :param string dest_gz_tmp: temporary local file path for the copy of 'src'
        Example:
            "/tmp/HepNames-records.xml.gz"

    :return: file content of 'src' or None, if no valid 'src'
    """
    xml_file_content = None

    try:
        # Save 'src' temporary to disk ('dest_gz_tmp')
        urllib.urlretrieve(src, dest_gz_tmp)
    except IOError as e:
        write_message(
            "Error: failed to copy '{0}' to '{1}'. ({2})".format(
                src, dest_gz_tmp, e),
            sys.stderr)
        return

    try:
        # Since we are running Python 2.6.6
        f = gzip.open(dest_gz_tmp, "rb")
        xml_file_content = f.read()
        f.close()
    except IOError as e:
        write_message(
            "Error: failed to unzip '{0}'. ({1})".format(dest_gz_tmp, e),
            sys.stderr)

    try:
        # Remove 'dest_gz_tmp' from disk
        os.remove(dest_gz_tmp)
    except OSError as e:
        write_message(
            "Error: failed to remove '{0}' from disk. ({1})".format(
                dest_gz_tmp, e),
            sys.stderr)

    return xml_file_content


def parse_inspire_xml(xml_content):
    """Parse xml_content and return a dictionary of authority ids.

    Consider records having a CCID and INSPIRE id.

    :param string xml_content: xml file content representing MARC XML records
        Example:
            '''
            <record>
                <!-- ... -->
                <datafield tag="035" ind1=" " ind2=" ">
                    <subfield code="9">INSPIRE</subfield>
                    <subfield code="a">INSPIRE-00440376</subfield>
                </datafield>
                <!-- ... -->
            </record>
            <record>
                <!-- ... -->
                <datafield tag="035" ind1=" " ind2=" ">
                    <subfield code="9">CERN</subfield>
                    <subfield code="a">CERN-389900</subfield>
                </datafield>
                <datafield tag="035" ind1=" " ind2=" ">
                    <subfield code="9">INSPIRE</subfield>
                    <subfield code="a">INSPIRE-00146525</subfield>
                </datafield>
                <!-- ... -->
            </record>
            <!-- ... -->
            '''

    :return: dictionary containing CERN and INSPIRE ids
        Example:
            {"CERN-389900": "INSPIRE-00146525", ...}
    """
    try:
        root = etree.fromstring(xml_content)
    except (ValueError, etree.XMLSyntaxError) as e:
        write_message(
            "Error: failed to parse XML content. ({0})".format(e),
            sys.stderr)
        return

    authority_ids = {}

    for record in root:
        inspire_id = None
        cern_id = None

        for datafield in record.xpath("datafield[@tag='035']"):
            if datafield.xpath("subfield[@code='9' and text()='INSPIRE']"):
                try:
                    inspire_id = datafield.find("subfield[@code='a']").text
                except AttributeError:
                    pass
            elif datafield.xpath("subfield[@code='9' and text()='CERN']"):
                try:
                    cern_id = datafield.find("subfield[@code='a']").text
                except AttributeError:
                    pass

        if inspire_id and cern_id:
            authority_ids[cern_id] = inspire_id

    return authority_ids


def get_ccid(record_id):
    """Get CCID of given record_id having no INSPIRE authority id.

    Consider authority ids stored in MARC field '035__a'. INSPIRE authority id
    is labeled with the prefix "AUTHOR|(INSPIRE)".

    :param int record_id: record id
        Example 1: MARC XML excerpt of a record having no INSPIRE authority id
            '''
            <datafield tag="035" ind1=" " ind2=" ">
                <subfield code="a">AUTHOR|(SzGeCERN)389900</subfield>
            </datafield>
            '''

        Example 2: MARC XML excerpt of a record having an INSPIRE authority id
            '''
            <datafield tag="035" ind1=" " ind2=" ">
                <subfield code="a">AUTHOR|(SzGeCERN)646446</subfield>
            </datafield>
            <datafield tag="035" ind1=" " ind2=" ">
                <subfield code="a">AUTHOR|(INSPIRE)INSPIRE-00198527</subfield>
            </datafield>
            '''

    :return: CCID of given record_id, 'None' if INSPIRE authority id has been
        found for this record
        Example 1:
            '389900'

        Example 2:
            'None'
    """
    cern_id = None
    # Consider records having no INSPIRE id
    for control_number in get_fieldvalues(record_id, "035__a"):
        if control_number.startswith("AUTHOR|(INSPIRE)"):
            break
        elif control_number.startswith("AUTHOR|(SzGeCERN)"):
            _, _, cern_id = control_number.partition("AUTHOR|(SzGeCERN)")

    return cern_id

def synchronize(record_ids, authority_ids, dest_xml):
    """Synchronize record_ids with authority_ids.

    :param list record_ids: list of record ids to synchronize
        Example:
            [2108556, 2148049]
    :param dict authority_ids: dictionary containing CERN and INSPIRE authority
        ids. Created by 'parse_inspire_xml'
        Example:
            {"CERN-389900": "INSPIRE-00146525"}
    :param string dest_xml: file path to write the updates to disk and send to
        bibuplaod
        Example:
            "cds-records-updates.xml"

            Output string:
            <record>
                <controlfield tag='001'>2108556</controlfield>
                <datafield tag='035' ind1=' ' ind2=' '>
                    <subfield code='a'>
                        AUTHOR|(INSPIRE)INSPIRE-00146525</subfield>
                </datafield>
            </record>
    """
    # String representation of a record element, containing controlfield '001'
    # and datafield '035__a' storing the record id and INSPIRE id
    record = ('<record>'
              '<controlfield tag="001">{0}</controlfield>'
              '<datafield tag="035" ind1=" " ind2=" ">'
              '<subfield code="a">AUTHOR|(INSPIRE){1}</subfield>'
              '</datafield>'
              '</record>')

    # Create output string
    output = ""
    for record_id in record_ids:
        cern_id = get_ccid(record_id)

        try:
            # Get INSPIRE authority id for given CCID, if available
            inspire_id = authority_ids["CERN-{0}".format(cern_id)]

            # Append record to the output string
            output += record.format(record_id, inspire_id)
        except KeyError:
            pass

    if output:
        # Dump updates to disk ('dest_xml')
        try:
            with open(dest_xml, "w") as f:
                f.write(output)

            write_message(
                "Info: updates have been written to '{0}'.".format(dest_xml))

            # Upload (--append) updates to CDS
            task_id = task_low_level_submission(
                "bibupload",  # Name
                "bibauthority-people",  # User
                "--append", dest_xml,
                "-P", "-1",
                "-N", "inspire-authority-ids-synchronizer")
            write_message(
                "Info: task '{0}' has been submitted to the scheduler".format(
                    task_id))
        except EnvironmentError as e:
            write_message(
                "Error: failed to write updates to '{0}'. ({1})".format(
                    dest_xml, e),
                sys.stderr)
    else:
        write_message("Info: no updates for records have been found")


def bst_inspire_authority_ids_synchronizer(
        url=SYNC_URL_INSPIRE_RECORDS_SRC, tmp_dir=SYNC_LOCAL_TMP_DIR):
    """Synchronize INSPIRE authority ids.

    :param string url: valid URL to the gzip (.gz) file
    :param string tmp_dir: existing directory path for temporary files
    """ 
    xml_content = get_inspire_dump(
        url, os.path.join(tmp_dir, SYNC_LOCAL_INSPIRE_RECORDS_FILE_NAME))

    authority_ids = parse_inspire_xml(xml_content)

    if authority_ids:
        record_ids = get_record_ids()
        write_message(
            "Info: {0} record ids have been requested".format(len(record_ids)))
        if record_ids:
            synchronize(
                record_ids,
                authority_ids,
                os.path.join(tmp_dir, SYNC_LOCAL_CDS_RECORDS_UPDATES_FILE_NAME))

