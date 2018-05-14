from dataservice.util.data_import.etl.load.db_load import Loader


class SeqCenterLoader(Loader):

    def run(self, *args, **kwargs):
        kwargs['entity_types'] = ['sequencing_center']
        return super().run(*args, **kwargs)

    @classmethod
    def drop_all(cls):
        from dataservice.api.sequencing_center.models import (
            SequencingCenter
        )
        from dataservice.extensions import db
        SequencingCenter.query.delete()
        db.session.commit()
