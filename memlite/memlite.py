# coding: utf-8
#
# BSD licence
#
import sys


class _Base(object):

    def __init__(self):
        self.fields = []

    def create(self, *fields):
        """
        Create a new base with given field names (and default values) like these:
        - [f1, f2, ...]
        - [{f1: default1}, {f2: default2}, ...}
        - [(f1, default1), (f2, default2), ...]
        """
        self.fields = []
        self.default_values = {}
        for field in fields:
            if type(field) is dict:
                self.fields.append(field["name"])
                self.default_values[field["name"]] = field.get("default", None)
            elif type(field) is tuple:
                self.fields.append(field[0])
                self.default_values[field[0]] = field[1]
            else:
                self.fields.append(field)
                self.default_values[field] = None

        self.records = {}
        self.next_id = 0
        self.indices = {}
        return self

    def create_index(self, *fields):
        """Create an index on the given field names"""
        for f in fields:
            if f not in self.fields:
                raise NameError("%s is not a field name %s" % (f, self.fields))
            self.indices[f] = {}
            for _id, record in self.records.items():
                self.indices[f].setdefault(record[f], set()).add(_id)

    def delete_index(self, *fields):
        """Delete the index on the given field names"""
        for f in fields:
            if f not in self.indices:
                raise ValueError("No index on field %s" % f)
        for f in fields:
            del self.indices[f]

    def insert(self, *args, **kw):
        """
        Insert one record in the database. The form of record could only be args or kw.
        - args (values, or a list/tuple of values): The record(s) to insert.
        - kw (dict): The field/values to insert
        """
        if args:
            kw = {f: arg for f, arg in zip(self.fields, args)}
        for key in kw:
            if key not in self.fields:
                raise NameError("Invalid field name : %s" % key)
        # To speed up without deepcopy
        # record = copy.deepcopy(self.default_values)
        record = kw
        for k in self.default_values.viewkeys() - kw.viewkeys():
            record[k] = self.default_values[k]
        record['__id__'] = self.next_id
        self.records[self.next_id] = record
        # update index
        for f in self.indices.iterkeys():
            self.indices[f].setdefault(record[f], set()).add(self.next_id)
        self.next_id += 1
        return record['__id__']

    def delete(self, remove):
        """
        Remove a single record, or the records in an iterable
        - remove (record or list of records): The record(s) to delete.
        """
        if isinstance(remove, dict):
            remove = [remove]
        else:
            # convert iterable into a list (to be able to sort it)
            remove = [r for r in remove]
        if not remove:
            return 0
        _ids = [r['__id__'] for r in remove]
        _ids.sort()
        recodes_ids = self.records.iterkeys()
        # check if the records are in the base
        if not set(_ids).issubset(recodes_ids):
            missing = list(set(_ids).difference(recodes_ids))
            raise IndexError('Delete aborted. Records with these ids'
                             ' not found in the base : %s' % str(missing))
        # raise exception if duplicate ids
        for i in xrange(len(_ids) - 1):
            if _ids[i] == _ids[i + 1]:
                raise IndexError("Delete aborted. Duplicate id : %s" % _ids[i])
        deleted = len(remove)
        while remove:
            r = remove.pop()
            _id = r['__id__']
            # remove id from indices
            for f in self.indices.iterkeys():
                self.indices[f][r[f]].remove(_id)
                if not self.indices[f][r[f]]:
                    del self.indices[f][r[f]]
            # remove record from self.records
            del self.records[_id]
        return deleted

    def update(self, records, **kw):
        # ignore unknown fields
        kw = {k: v for k, v in kw.iteritems() if k in self.fields}
        if isinstance(records, dict):
            records = [records]
        # update indices
        for f in self.indices.viewkeys() & kw.viewkeys():
            for record in records:
                if record[f] == kw[f]:
                    continue
                _id = record["__id__"]
                # remove id for the old value
                self.indices[f][record[f]].remove(_id)
                if not self.indices[f][record[f]]:
                    del self.indices[f][record[f]]
                # insert new value
                self.indices[f].setdefault(kw[f], set()).add(_id)
        for record in records:
            record.update(kw)

    def add_field(self, field, default=None):
        """Adds a field to the database"""
        if field in self.fields + ["__id__", "__version__"]:
            raise ValueError("Field %s already defined" % field)
        if not hasattr(self, 'records'):  # base not open yet
            self.create((field, default))
        for r in self:
            r[field] = default
        self.fields.append(field)
        self.default_values[field] = default

    def drop_field(self, field):
        """Removes a field from the database"""
        if field in ["__id__", "__version__"]:
            raise ValueError("Can't delete field %s" % field)
        self.fields.remove(field)
        for r in self:
            del r[field]
        if field in self.indices:
            del self.indices[field]

    def query(self, *args, **kw):
        if args and kw:
            raise SyntaxError("Can't specify positional AND keyword arguments")

        if args:
            if len(args) > 1:
                raise SyntaxError("Only one field can be specified")
            elif args[0] not in self.fields:
                raise ValueError("%s is not a field" % args[0])
        if not kw:
            return self.records.values()

        # indices and non-indices
        keys = kw.viewkeys()
        indexs = keys & self.indices.viewkeys()
        no_indexs = keys - indexs
        if indexs:
            # fast selection on indices
            field = indexs.pop()
            res = self.indices[field].get(kw[field], set())
            if not res:
                return []
            while indexs:
                field = indexs.pop()
                res = res & self.indices[field].get(kw[field], set())
        else:
            # if no index, initialize result with test on first field
            field = no_indexs.pop()
            res = {record["__id__"] for record in self if record[field] == kw[field]}
        # selection on non-index fields
        for field in no_indexs:
            res = res & {_id for _id in res if self.records[_id][field] == kw[field]}
        return [self[_id] for _id in res]

    def __getitem__(self, key):
        return self.records[key]

    def __len__(self):
        return len(self.records)

    def __delitem__(self, record_id):
        """Delete by record id"""
        self.delete(self[record_id])

    def __contains__(self, record_id):
        return record_id in self.records

    def get_indices(self):
        """Returns the indices"""
        return self.indices


class _BasePy2(_Base):

    def __iter__(self):
        """Iteration on the records"""
        return self.records.itervalues()


if sys.version_info[0] == 2:
    Base = _BasePy2
else:
    raise RuntimeError('No support for Python3')
