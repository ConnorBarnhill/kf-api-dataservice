from flask import abort, request
from marshmallow import ValidationError

from dataservice.extensions import db
from dataservice.api.common.pagination import paginated, Pagination
from dataservice.api.study_file.models import StudyFile
from dataservice.api.study_file.schemas import StudyFileSchema
from dataservice.api.common.views import CRUDView


class StudyFileListAPI(CRUDView):
    """
    Study File API
    """
    endpoint = 'study_files_list'
    rule = '/study-files'
    schemas = {'StudyFile': StudyFileSchema}

    @paginated
    def get(self, after, limit):
        """
        Get a paginated study files
        ---
        template:
          path:
            get_list.yml
          properties:
            resource:
              StudyFile
        """
        q = StudyFile.query

        # Filter by study
        study_id = request.args.get('study_id')
        if study_id:
            q = q.filter(StudyFile.study_id == study_id)

        pager = Pagination(q, after, limit)
        keep = []
        refresh = True
        next_after = None
        # Continue updating the page until we get a page with no deleted files
        while (pager.total > 0 and refresh):
            refresh = False
            # Move the cursor ahead to the last valid file
            next_after = keep[-1].created_at if len(keep) > 0 else after
            # Number of results needed to fulfill the original limit
            remain = limit - len(keep)
            pager = Pagination(q, next_after, remain)

            for st in pager.items:
                merged = st.merge_indexd()
                if merged is not None:
                    keep.append(st)
                else:
                    refresh = True

        # Replace original page's items with new list of valid files
        pager.items = keep
        pager.after = next_after if next_after else after

        return (StudyFileSchema(many=True)
                .jsonify(pager))

    def post(self):
        """
        Create a new study_file
        ---
        template:
          path:
            new_resource.yml
          properties:
            resource:
              StudyFile
        """
        try:
            st = StudyFileSchema(strict=True).load(request.json).data
        except ValidationError as err:
            abort(400, 'could not create study_file: {}'.format(err.messages))
        db.session.add(st)
        db.session.commit()
        return StudyFileSchema(
            201, 'study_file {} created'.format(st.kf_id)
        ).jsonify(st), 201


class StudyFileAPI(CRUDView):
    """
    StudyFile API
    """
    endpoint = 'study_files'
    rule = '/study-files/<string:kf_id>'
    schemas = {'StudyFile': StudyFileSchema}

    def get(self, kf_id):
        """
        Get a study_file by id
        ---
        template:
          path:
            get_by_id.yml
          properties:
            resource:
              StudyFile
        """
        st = StudyFile.query.get(kf_id)
        if st is None:
            abort(404, 'could not find {} `{}`'
                  .format('study_file', kf_id))

        # Merge will return None if the document wasnt found in indexd
        merge = st.merge_indexd()
        if merge is None:
            abort(404, 'could not find {} `{}`'
                  .format('study_file', kf_id))

        return StudyFileSchema(many=False).jsonify(st)

    def patch(self, kf_id):
        """
        Update an existing study_file. Allows partial update of resource
        ---
        template:
          path:
            update_by_id.yml
          properties:
            resource:
              StudyFile
        """
        body = request.json
        st = StudyFile.query.get(kf_id)
        if st is None:
            abort(404, 'could not find {} `{}`'
                  .format('study_file', kf_id))

        # Fetch fields from indexd first
        merge = st.merge_indexd()
        if merge is None:
            abort(404, 'could not find {} `{}`'
                  .format('study_file', kf_id))

        # Deserialization will require this field and won't merge automatically
        if 'study_id' not in body:
            body['study_id'] = st.study_id

        try:
            st = (StudyFileSchema(strict=True).load(body, instance=st,
                                                    partial=True).data)
        except ValidationError as err:
            abort(400, 'could not update study_file: {}'.format(err.messages))

        db.session.add(st)
        db.session.commit()

        return StudyFileSchema(
            200, 'study_file {} updated'.format(st.kf_id)
        ).jsonify(st), 200

    def delete(self, kf_id):
        """
        Delete study_file by id
        ---
        template:
          path:
            delete_by_id.yml
          properties:
            resource:
              StudyFile
        """
        st = StudyFile.query.get(kf_id)
        if st is None:
            abort(404, 'could not find {} `{}`'.format('study_file', kf_id))

        db.session.delete(st)
        db.session.commit()

        return StudyFileSchema(
            200, 'study_file {} deleted'.format(st.kf_id)
        ).jsonify(st), 200
