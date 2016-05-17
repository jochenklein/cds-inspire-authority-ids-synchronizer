"""Set of test cases for the synchronizer.

Notes:
    * requires read and write permission to invenio.config.CFG_TMPDIR
    * requires a local copy (excerpt) 'HepNames-records-excerpt.xml' of the
      unzipped SYNC_INSPIRE_RECORDS_URL_SRC
"""

import os.path
import unittest

import bst_inspire_authority_ids_synchronizer as sync


class TestSynchronizer(unittest.TestCase):

    """Set of test cases for the synchronizer."""

    def test_local_tmp_dir(self):
        """Test default temporary directory if exist."""
        self.assertTrue(os.path.isdir(sync.SYNC_LOCAL_TMP_DIR))

    def test_get_inspire_dump(self):
        """Test given the default URL and local file path."""
        url = sync.SYNC_URL_INSPIRE_RECORDS_SRC
        tmp = os.path.join(
            sync.SYNC_LOCAL_TMP_DIR,
            sync.SYNC_LOCAL_INSPIRE_RECORDS_FILE_NAME)

        xml_file_content = sync.get_inspire_dump(url, tmp)

        self.assertTrue(xml_file_content)
        self.assertFalse(os.path.isfile(tmp))

    def test_get_inspire_dump_invalid_url(self):
        """Test given a invalid URL."""
        url = "http://inspirehep.net/dumps/HepNames-records.xml.gzzzz"
        tmp = os.path.join(
            sync.SYNC_LOCAL_TMP_DIR,
            sync.SYNC_LOCAL_INSPIRE_RECORDS_FILE_NAME)

        xml_file_content = sync.get_inspire_dump(url, tmp)

        self.assertEqual(xml_file_content, None)
        self.assertFalse(os.path.isfile(tmp))

    def test_get_inspire_dump_invalid_file_path(self):
        """Test given a invalid local file path."""
        url = sync.SYNC_URL_INSPIRE_RECORDS_SRC
        tmp = "/tmp/"

        xml_file_content = sync.get_inspire_dump(url, tmp)

        self.assertEqual(xml_file_content, None)
        self.assertFalse(os.path.isfile(tmp))

    def test_parse_inspire_xml(self):
        """Test 'xml_content' as valid XML."""
        with open("HepNames-records-excerpt.xml") as f:
            xml_content = f.read()
        authority_ids = sync.parse_inspire_xml(xml_content)

        self.assertEqual(authority_ids["CERN-389900"], "INSPIRE-00146525")
        self.assertEqual(authority_ids["CERN-389882"], "INSPIRE-00079322")
        self.assertEqual(authority_ids["CERN-389853"], "INSPIRE-00079313")
        self.assertEqual(authority_ids["CERN-389849"], "INSPIRE-00079305")

        xml_content = "<record></record>"
        authority_ids = sync.parse_inspire_xml(xml_content)
        self.assertEqual(authority_ids, {})

    def test_parse_inspire_xml_no_xml_content(self):
        """Test 'xml_content' as 'None'."""
        xml_content = None
        authority_ids = sync.parse_inspire_xml(xml_content)
        self.assertEqual(authority_ids, None)

    def test_parse_inspire_xml_empty_xml_content(self):
        """Test 'xml_content' as empty string ('')."""
        xml_content = ""
        authority_ids = sync.parse_inspire_xml(xml_content)
        self.assertEqual(authority_ids, None)

    def test_parse_inspire_xml_invalid_xml_content(self):
        """Test 'xml_content' as invalid XML."""
        xml_content = ">record<"
        authority_ids = sync.parse_inspire_xml(xml_content)
        self.assertEqual(authority_ids, None)

    def test_get_record_ids(self):
        """Test the list of record ids for a given record id."""
        record_ids = sync.get_record_ids()
        self.assertTrue(2108556 in record_ids)
        self.assertTrue(2148049 in record_ids)
        
    def test_get_ccid(self):
        """Test 'get_ccid' for a given authority record, having no INSPIRE id

        Using CDS record with id '2108556' which represents the authority record
        of 'Jonathan R. Ellis'. It stores the CCID 'AUTHOR|(SzGeCERN)389900' in
        MARC field '035__a'. No INSPIRE authority id is stored in this record.
        """
        cern_id = sync.get_ccid(2108556)
        self.assertEqual(cern_id, "389900")

    def test_get_ccid_with_inspire_id(self):
        """Test 'get_ccid' for a given authority record, having INSPIRE id

        Using CDS record with id '2148049' which represents the authority record
        of 'Hossain Ahmed'. It stores the CCID 'AUTHOR|(SzGeCERN)646446' in
        MARC field '035__a'. An INSPIRE authority id is stored in this record.
        """
        cern_id = sync.get_ccid(2148049)
        self.assertEqual(cern_id, None)


if __name__ == '__main__':
    unittest.main()

