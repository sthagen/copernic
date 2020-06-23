# copernic (https://github.com/amirouche/copernic)

# Copyright (C) 2020 Amirouche Boubekki

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.

# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
from uuid import uuid4

import fdb
import fdb.tuple
import xxhash


class IStoreBase:
    pass


class IStoreException(Exception):
    pass



HASH_TO_UUID = 0
UUID_TO_HASH = 1
UUID_TO_VALUE = 2
MAGIC = 42


def get_or_create(tr, value):
    bytes = fdb.tuple.pack((value,))
    hash = xxhash.xxh64_digest(bytes)
    key = fdb.tuple.pack((MAGIC, HASH_TO_UUID, hash))
    uid = tr.get(key)
    if uid is not None:
        return uid
    # otherwise create it
    uid = uuid4()
    tr.set(key, uid)
    tr.set(fdb.tuple.pack((MAGIC, UUID_TO_HASH, uid)), hash)
    tr.set(fdb.tuple.pack((MAGIC, UUID_TO_VALUE, uid)), bytes)
    return uid
