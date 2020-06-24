import json
from uuid import UUID

import rdflib
import fdb
from django.core.management.base import BaseCommand, CommandError

import vnstore
import nstore
import istore
from frontend.models import ChangeRequest


fdb.api_version(620)
db = fdb.open()


ITEMS = ['uid', 'key', 'value']

var = nstore.var
# vnstore contains the versioned ITEMS
vnstore = vnstore.open(['copernic', 'vnstore'], ITEMS)


class Command(BaseCommand):

    def add_arguments(self, parser):
        parser.add_argument('format')
        parser.add_argument('filename')
        parser.add_argument('message')

    def handle(self, *args, **kwargs):
        format = kwargs['format']
        filename = kwargs['filename']
        message = kwargs['message']

        file = open(filename)

        subspace = fdb.subspace_impl.Subspace(('hca',))
        allocator = fdb.directory_impl.HighContentionAllocator()

        @fdb.transactional
        def change_create(tr, message):
            changeid = vnstore.change_create(tr)
            vnstore.change_message(tr, changeid, message)
            change = ChangeRequest(changeid=changeid, message=message)
            change.save()
            return changeid

        changeid = change_create(db, message)

        @fdb.transactional
        def save(tr, changeid, uid, key, value):
            vnstore.change_continue(tr, changeid)
            vnstore.add(tr, uid, key, value)

        def simplify(v):
            if isinstance(v, (int, float)):
                return v
            return str(v)

        @fdb.transactional
        def load(tr):
            for index, line in enumerate(file):
                if index % 1_000 == 0:
                    print(index)

                g = rdflib.Graph()
                g.parse(data=line, format=format)
                uid, key, value = next(iter(g))

                if isinstance(uid, str):
                    uid = istore.get_or_create(tr, allocator, simplify(uid))
                if isinstance(key, str):
                    key = istore.get_or_create(tr, allocator, simplify(key))
                if isinstance(value, str):
                    value = istore.get_or_create(tr, allocator, simplify(value))

                print(uid, key, value)
                save(tr, changeid, uid, key, value)

        load(db)

        @fdb.transactional
        def apply(tr, change, changeid):
            # apply change to vnstore
            vnstore.change_apply(tr, changeid)
            # mark the change as applied
            change.status = ChangeRequest.STATUS_APPLIED
            change.save()

        change = ChangeRequest.objects.get(changeid=changeid)
        apply(db, change, changeid)
