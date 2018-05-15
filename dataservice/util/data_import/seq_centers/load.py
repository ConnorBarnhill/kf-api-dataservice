import requests
from dataservice.util.data_import.etl.load.db_load import (
    Loader as BaseDBLoader
)
from dataservice.util.data_import.etl.load.kf_dataservice_load import (
    Loader as BaseDataserviceLoader
)


class DbLoader(BaseDBLoader):

    def drop_all(self):
        from dataservice.api.sequencing_center.models import (
            SequencingCenter
        )
        from dataservice.extensions import db
        SequencingCenter.query.delete()
        db.session.commit()


class DataserviceLoader(BaseDataserviceLoader):

    def drop_all(self):
        """
        Delete all sequencing_centers
        """
        # Get seq centers
        url = self.base_url + '/sequencing-centers'
        headers = {'Content-Type': 'application/json'}
        response = requests.get(url, headers=headers)
        if response.status_code > 300:
            print('Aborting! {}'.format(response.text))
            return

        # Delete all seq center
        for sc in response.json()['results']:
            kf_id = sc['kf_id']
            url = self.base_url + '/sequencing-centers/{}'.format(kf_id)
            headers = {'Content-Type': 'application/json'}
            response = requests.delete(url, headers=headers)
            if response.status_code > 300:
                print('Failed! {}'.format(response.text))
                return
