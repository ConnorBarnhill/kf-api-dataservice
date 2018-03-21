import json
import requests
import pytest
from unittest.mock import MagicMock
from requests.exceptions import HTTPError

from flask import url_for

from dataservice.extensions import db
from dataservice.api.genomic_file.models import GenomicFile
from dataservice.api.sequencing_experiment.models import SequencingExperiment


GENOMICFILE_URL = 'api.genomic_files'
GENOMICFILE_LIST_URL = 'api.genomic_files_list'


@pytest.fixture
def genomic_files(client, entities):

    props = {
        'file_name': 'hg38.bam',
        'data_type': 'aligned reads',
        'sequencing_experiment_id': SequencingExperiment.query.first().kf_id,
        'file_format': 'bam'
    }
    gfs = [GenomicFile(**props) for _ in range(102)]
    db.session.add_all(gfs)
    db.session.commit()


def test_new(client, entities):
    """
    Test creating a new genomic file
    """
    resp = _new_genomic_file(client)
    assert 'genomic_file' in resp['_status']['message']
    assert 'created' in resp['_status']['message']
    assert resp['results']['file_name'] == 'hg38.bam'

    genomic_file = resp['results']
    assert GenomicFile.query.get(genomic_file['kf_id'])


def test_new_indexd_error(client, entities):
    """
    Test case when indexd errors
    """

    body = {
        'file_name': 'hg38.bam',
        'size': 123,
        'data_type': 'aligned reads',
        'file_format': 'bam',
        'urls': ['s3://bucket/key'],
        'hashes': {'md5': 'd418219b883fce3a085b1b7f38b01e37'},
        'sequencing_experiment_id': 'SE_AAAAAAAA',
        'controlled_access': False
    }
    init_count = GenomicFile.query.count()
    response = client.post(url_for(GENOMICFILE_LIST_URL),
                                headers={'Content-Type': 'application/json'},
                                data=json.dumps(body))
    resp = json.loads(response.data.decode("utf-8"))

    assert 'does not exist' in resp['_status']['message']
    assert GenomicFile.query.count() == init_count


def test_get_list(client, genomic_files):
    """
    Test that genomic files are returned in a paginated list with all
    info loaded from indexd
    """

    resp = client.get(url_for(GENOMICFILE_LIST_URL))
    resp = json.loads(resp.data.decode('utf-8'))

    assert resp['_status']['code'] == 200
    assert resp['total'] == GenomicFile.query.count()
    assert len(resp['results']) == 10


def test_get_one(client, entities):
    """
    Test that genomic files are returned in a paginated list with all
    info loaded from indexd
    """
    
    gf = GenomicFile.query.first()
    gf.merge_indexd()

    resp = client.get(url_for(GENOMICFILE_URL, kf_id=gf.kf_id))
    resp = json.loads(resp.data.decode('utf-8'))

    assert resp['_status']['code'] == 200
    resp = resp['results']
    # check properties from indexd
    assert resp['hashes'] == gf.hashes
    assert resp['metadata'] == gf._metadata
    assert 'rev' not in resp
    assert resp['size'] == gf.size
    # check properties from datamodel
    assert resp['file_name'] == gf.file_name
    assert resp['data_type'] == gf.data_type
    assert resp['file_format'] == gf.file_format


def test_update(client, indexd, entities):
    """
    Test updating an existing genomic file

    This will should create a new version in indexd
    """

    resp = _new_genomic_file(client)
    orig_calls = indexd.post.call_count
    participant = resp['results']
    kf_id = participant.get('kf_id')
    orig = resp['results']

    body = {
        'file_name': 'hg37.bam',
        'size': 23498
    }
    response = client.patch(url_for(GENOMICFILE_URL,
                                  kf_id=kf_id),
                               data=json.dumps(body),
                            headers={'Content-Type': 'application/json'})

    assert indexd.post.call_count == orig_calls + 1

    assert response.status_code == 200

    resp = json.loads(response.data.decode("utf-8"))
    assert 'genomic_file' in resp['_status']['message']
    assert 'updated' in resp['_status']['message']

    gf = resp['results']
    assert gf['kf_id'] == kf_id
    assert gf['file_name'] == body['file_name']

    gf = GenomicFile.query.first()
    gf.merge_indexd()
    assert gf.file_name, body['file_name']


def test_delete(client, indexd, entities):
    """
    Test deleting a participant by id
    """
    init = GenomicFile.query.count()

    r = _new_genomic_file(client)
    kf_id = r['results']['kf_id']

    response = client.delete(url_for(GENOMICFILE_URL,
                                     kf_id=kf_id),
                             headers={'Content-Type': 'application/json'})

    resp = json.loads(response.data.decode("utf-8"))

    assert 'genomic_file' in resp['_status']['message']
    assert 'deleted' in resp['_status']['message']
    assert GenomicFile.query.count() == init
    assert indexd.delete.call_count == 1


def test_delete_error(client, indexd, entities):
    """
    Test handling of indexd error
    """

    response_mock = MagicMock()
    response_mock.status_code = 500
    response_mock.json.return_value = {'error': 'fake error message'}
    def exc():
        raise HTTPError()
    response_mock.raise_for_status = exc
    indexd.delete.return_value = response_mock

    init = GenomicFile.query.count()

    r = _new_genomic_file(client)
    kf_id = r['results']['kf_id']

    response = client.delete(url_for(GENOMICFILE_URL,
                                     kf_id=kf_id),
                             headers={'Content-Type': 'application/json'})

    resp = json.loads(response.data.decode("utf-8"))

    assert indexd.delete.call_count == 1
    assert 'could not delete record' in resp['_status']['message']
    assert 'fake error message' in resp['_status']['message']
    assert GenomicFile.query.count() == init + 1


def _new_genomic_file(client):
    """ Creates a genomic file """
    body = {
        'file_name': 'hg38.bam',
        'size': 123,
        'data_type': 'aligned reads',
        'file_format': 'bam',
        'urls': ['s3://bucket/key'],
        'hashes': {'md5': 'd418219b883fce3a085b1b7f38b01e37'},
        'sequencing_experiment_id': SequencingExperiment.query.first().kf_id,
        'controlled_access': False
    }
    response = client.post(url_for(GENOMICFILE_LIST_URL),
                                headers={'Content-Type': 'application/json'},
                                data=json.dumps(body))
    resp = json.loads(response.data.decode("utf-8"))
    assert response.status_code == 201
    return resp
