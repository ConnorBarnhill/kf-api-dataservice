import json
from flask import url_for
from urllib.parse import urlparse
from datetime import datetime
from dateutil import parser, tz

from dataservice.extensions import db
from dataservice.api.common import id_service
from dataservice.api.diagnosis.models import Diagnosis
from dataservice.api.participant.models import Participant
from dataservice.api.study.models import Study
from tests.utils import FlaskTestCase

DIAGNOSES_URL = 'api.diagnoses'
DIAGNOSES_LIST_URL = 'api.diagnoses_list'


class DiagnosisTest(FlaskTestCase):
    """
    Test diagnosis api
    """

    def test_post(self):
        """
        Test create a new diagnosis
        """
        kwargs = self._create_save_to_db()

        # Create diagnosis data
        kwargs = {
            'external_id': 'd1',
            'diagnosis': 'flu',
            'age_at_event_days': 365,
            'diagnosis_category': 'cancer',
            'tumor_location': 'Brain',
            'mondo_id': 'DOID:8469',
            'icd_id': 'J10.01',
            'uberon_id':'UBERON:0000955',
            'participant_id': kwargs.get('participant_id')
        }
        # Send get request
        response = self.client.post(url_for(DIAGNOSES_LIST_URL),
                                    data=json.dumps(kwargs),
                                    headers=self._api_headers())

        # Check response status status_code
        self.assertEqual(response.status_code, 201)

        # Check response content
        response = json.loads(response.data.decode('utf-8'))
        diagnosis = response['results']
        dg = Diagnosis.query.get(diagnosis.get('kf_id'))
        for k, v in kwargs.items():
            if k == 'participant_id':
                continue
            self.assertEqual(diagnosis[k], getattr(dg, k))
        self.assertEqual(2, Diagnosis.query.count())

    def test_post_missing_req_params(self):
        """
        Test create diagnosis that is missing required parameters in body
        """
        # Create diagnosis data
        kwargs = {
            'external_id': 'd1',
            'diagnosis': 'flu',
            'age_at_event_days': 365,
            'diagnosis_category': 'cancer',
            'tumor_location': 'Brain',
            'mondo_id': 'DOID:8469',
            'uberon_id':'UBERON:0000955',
            'icd_id': 'J10.01'
            # missing required param participant_id
        }
        # Send post request
        response = self.client.post(url_for(DIAGNOSES_LIST_URL),
                                    headers=self._api_headers(),
                                    data=json.dumps(kwargs))

        # Check status code
        self.assertEqual(response.status_code, 400)
        # Check response body
        response = json.loads(response.data.decode("utf-8"))
        # Check error message
        message = 'could not create diagnosis'
        self.assertIn(message, response['_status']['message'])
        # Check field values
        d = Diagnosis.query.first()
        self.assertIs(d, None)

    def test_post_invalid_age(self):
        """
        Test create diagnosis with bad input data

        Participant with participant_id does not exist in db
        """
        # Create diagnosis data
        kwargs = {
            'external_id': 'd1',
            'diagnosis': 'flu',
            'diagnosis_category': 'cancer',
            'tumor_location': 'Brain',
            # should be a positive integer
            'age_at_event_days': -5,
            'mondo_id': 'DOID:8469',
            'icd_id': 'J10.01',
            'uberon_id':'UBERON:0000955'
        }
        # Send post request
        response = self.client.post(url_for(DIAGNOSES_LIST_URL),
                                    headers=self._api_headers(),
                                    data=json.dumps(kwargs))

        # Check status code
        self.assertEqual(response.status_code, 400)

        # Check response body
        response = json.loads(response.data.decode("utf-8"))
        # Check error message
        message = 'could not create diagnosis'
        self.assertIn(message, response['_status']['message'])
        # Check field values
        d = Diagnosis.query.first()
        self.assertIs(d, None)

    def test_post_bad_input(self):
        """
        Test create diagnosis with bad input data

        Participant with participant_id does not exist in db
        """
        # Create diagnosis data
        kwargs = {
            'external_id': 'd1',
            'diagnosis': 'flu',
            'age_at_event_days': 365,
            'diagnosis_category': 'cancer',
            'tumor_location': 'Brain',
            'mondo_id': 'DOID:8469',
            'icd_id': 'J10.01',
            'uberon_id':'UBERON:0000955',
            # kf_id does not exist
            'participant_id': id_service.kf_id_generator('PT')()
        }
        # Send post request
        response = self.client.post(url_for(DIAGNOSES_LIST_URL),
                                    headers=self._api_headers(),
                                    data=json.dumps(kwargs))

        # Check status code
        self.assertEqual(response.status_code, 400)

        # Check response body
        response = json.loads(response.data.decode("utf-8"))
        # Check error message
        message = '"{}" does not exist'.format(kwargs['participant_id'])
        self.assertIn(message, response['_status']['message'])
        # Check field values
        d = Diagnosis.query.first()
        self.assertIs(d, None)

    def test_post_multiple(self):
        # Create a diagnosis with participant
        d1 = self._create_save_to_db()
        # Create another diagnosis for the same participant
        d2 = {
            'external_id': 'd2',
            'diagnosis': 'cold',
            'diagnosis_category': 'cancer',
            'tumor_location': 'Brain',
            'mondo_id': 'DOID:8469',
            'icd_id': 'J10.01',
            'uberon_id':'UBERON:0000955',
            'participant_id': d1['participant_id']
        }
        # Send post request
        response = self.client.post(url_for(DIAGNOSES_LIST_URL),
                                    headers=self._api_headers(),
                                    data=json.dumps(d2))
        # Check status code
        self.assertEqual(response.status_code, 201)
        # Check database
        c = Diagnosis.query.count()
        self.assertEqual(c, 2)
        pd = Participant.query.all()[0].diagnoses
        self.assertEqual(len(pd), 2)

    def test_get(self):
        # Create and save diagnosis to db
        kwargs = self._create_save_to_db()
        # Send get request
        response = self.client.get(url_for(DIAGNOSES_URL,
                                           kf_id=kwargs['kf_id']),
                                   headers=self._api_headers())

        # Check response status code
        self.assertEqual(response.status_code, 200)
        # Check response content
        response = json.loads(response.data.decode('utf-8'))
        diagnosis = response['results']
        participant_link = response['_links']['participant']
        participant_id = urlparse(participant_link).path.split('/')[-1]
        for k, v in kwargs.items():
            if k == 'participant_id':
                self.assertEqual(participant_id,
                                 kwargs['participant_id'])
            else:
                self.assertEqual(diagnosis[k], diagnosis[k])

    def test_get_all(self):
        """
        Test retrieving all diagnoses
        """
        kwargs = self._create_save_to_db()

        response = self.client.get(url_for(DIAGNOSES_LIST_URL),
                                   headers=self._api_headers())
        self.assertEqual(response.status_code, 200)
        response = json.loads(response.data.decode("utf-8"))
        content = response.get('results')
        self.assertEqual(len(content), 1)

    def test_patch(self):
        """
        Test updating an existing diagnosis
        """
        kwargs = self._create_save_to_db()
        kf_id = kwargs.get('kf_id')

        # Update existing diagnosis
        body = {
            'diagnosis': 'hangry',
            'diagnosis_category': 'birth defect',
            'participant_id': kwargs['participant_id']
        }
        response = self.client.patch(url_for(DIAGNOSES_URL,
                                             kf_id=kf_id),
                                     headers=self._api_headers(),
                                     data=json.dumps(body))
        # Status code
        self.assertEqual(response.status_code, 200)

        # Message
        resp = json.loads(response.data.decode("utf-8"))
        self.assertIn('diagnosis', resp['_status']['message'])
        self.assertIn('updated', resp['_status']['message'])

        # Content - check only patched fields are updated
        diagnosis = resp['results']
        dg = Diagnosis.query.get(kf_id)
        for k, v in body.items():
            self.assertEqual(v, getattr(dg, k))
        # Content - Check remaining fields are unchanged
        unchanged_keys = (set(diagnosis.keys()) -
                          set(body.keys()))
        for k in unchanged_keys:
            val = getattr(dg, k)
            if isinstance(val, datetime):
                d = val.replace(tzinfo=tz.tzutc())
                self.assertEqual(str(parser.parse(diagnosis[k])), str(d))
            else:
                self.assertEqual(diagnosis[k], val)

        self.assertEqual(1, Diagnosis.query.count())

    def test_patch_bad_input(self):
        """
        Test updating an existing participant with invalid input
        """
        kwargs = self._create_save_to_db()
        kf_id = kwargs.get('kf_id')
        body = {
            'participant_id': 'AAAA1111'
        }
        response = self.client.patch(url_for(DIAGNOSES_URL,
                                             kf_id=kf_id),
                                     headers=self._api_headers(),
                                     data=json.dumps(body))
        # Check status code
        self.assertEqual(response.status_code, 400)
        # Check response body
        response = json.loads(response.data.decode("utf-8"))
        # Check error message
        message = 'participant "AAAA1111" does not exist'
        self.assertIn(message, response['_status']['message'])
        # Check that properties are unchanged
        dg = Diagnosis.query.first()
        for k, v in kwargs.items():
            if k == 'participant_id':
                continue
            self.assertEqual(v, getattr(dg, k))

    def test_patch_missing_req_params(self):
        """
        Test create diagnosis that is missing required parameters in body
        """
        # Create and save diagnosis to db
        kwargs = self._create_save_to_db()
        kf_id = kwargs.get('kf_id')
        # Create diagnosis data
        body = {
            'diagnosis': 'hangry and flu'
        }
        # Send put request
        response = self.client.patch(url_for(DIAGNOSES_URL,
                                             kf_id=kwargs['kf_id']),
                                     headers=self._api_headers(),
                                     data=json.dumps(body))
        # Check status code
        self.assertEqual(response.status_code, 200)
        # Check response body
        response = json.loads(response.data.decode("utf-8"))
        # Check field values
        dg = Diagnosis.query.get(kf_id)
        for k, v in body.items():
            self.assertEqual(v, getattr(dg, k))

    def test_delete(self):
        """
        Test delete an existing diagnosis
        """
        kwargs = self._create_save_to_db()
        # Send get request
        response = self.client.delete(url_for(DIAGNOSES_URL,
                                              kf_id=kwargs['kf_id']),
                                      headers=self._api_headers())
        # Check status code
        self.assertEqual(response.status_code, 200)
        # Check response body
        response = json.loads(response.data.decode("utf-8"))
        # Check database
        d = Diagnosis.query.first()
        self.assertIs(d, None)

    def _create_save_to_db(self):
        """
        Create and save diagnosis

        Requires creating a participant
        Create a diagnosis and add it to participant as kwarg
        Save participant
        """
        # Create study
        study = Study(external_id='phs001')

        # Create diagnosis
        kwargs = {
            'external_id': 'd1',
            'diagnosis': 'flu',
            'diagnosis_category': 'cancer',
            'tumor_location': 'Brain',
            'age_at_event_days': 365,
            'mondo_id': 'DOID:8469',
            'icd_id': 'J10.01',
            'uberon_id':'UBERON:0000955'
        }
        d = Diagnosis(**kwargs)

        # Create and save participant with diagnosis
        participant_id = 'Test subject 0'
        p = Participant(external_id=participant_id, diagnoses=[d],
                        is_proband=True, study=study)
        db.session.add(p)
        db.session.commit()

        kwargs['participant_id'] = p.kf_id
        kwargs['kf_id'] = d.kf_id

        return kwargs
