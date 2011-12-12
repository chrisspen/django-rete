import os
import sys
import hashlib
import traceback
import base64
import re
import time
import tempfile
import cPickle as pickle
import datetime
import base64
from collections import defaultdict

from django.conf import settings
from django.core.files import File
from django.db import models
from django.db import IntegrityError, transaction, connection
from django.db.models import F
from django.db.models.query_utils import Q
from django.contrib.auth.models import User
from django.contrib.contenttypes import generic
from django.contrib.contenttypes.models import ContentType

try:
    import uuid
except ImportError:
    from django.utils import uuid

from uuidfield import UUIDField

from constants import S, P, O, BELONGSTOGRAPH, ANY, MAX_LENGTH

def remove_duplicates(lst):
    """
    Removes duplicates from a list, while maintaining order.
    """
    seen = set()
    next = list()
    for el in lst:
        if not el in seen:
            seen.add(el)
            next.append(el)
    return next

def getDictCursor():
    db = settings.DATABASES['default']
    engine = db['ENGINE'].split('.')[-1]
    if engine == 'mysql':
        import MySQLdb.cursors
        return connection.cursor(MySQLdb.DictCursor)
    elif engine == 'postgresql_psycopg2':
        # Django's db.connection doesn't support DictCursor.
        import psycopg2
        import psycopg2.extras
        connection = psycopg2.connect("dbname='%s' user='%s' password='%s'" % (
                                        db['NAME'],
                                        db['USER'],
                                        db['PASSWORD'],))
        #connection.autocommit(1)#todo:enable when psycopg2 upgraded
        return connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
    else:
        # Otherwise, simulate a DictCursor by wrapping the cursor instance.
        class _DictCursor:
            def __init__(self):
                from django.db import connection
                self.cursor = connection.cursor()
                self._results = None
            def execute(self, *args, **kwargs):
                self.cursor.execute(*args, **kwargs)
                desc = self.cursor.description
                self._results = [dict(zip([col[0] for col in desc], row)) for row in self.cursor.fetchall()]
            def __iter__(self):
                if self._results is not None:
                    for row in self._results:
                        yield row
        return _DictCursor()
        #raise Exception, 'Unknown database engine: %s' % db['ENGINE']

class UUIDVersionError(Exception):
    pass

class _BaseModel(models.Model):
    
    class Meta:
        abstract = True
        
    def save(self, *args, **kwargs):
        super(_BaseModel, self).save(*args, **kwargs)
        return self

#class Atom(_BaseModel):
#    text = models.TextField(blank=True, null=False)
#    hash = models.CharField(max_length=128, blank=False, null=False, unique=True)
#    
#    def save(self, *args, **kwargs):
#        self.hash = hashlib.sha512(self.text).hexdigest()
#        return super(Atom, self).save(*args, **kwargs)

class Atom(_BaseModel):
    text = models.CharField(max_length=MAX_LENGTH, blank=True, null=False, db_index=True, unique=True)

class TripleManager(models.Manager):
    def search(self, subject=None, predicate=None, object=None, gid=None, **kwargs):
        """
        Queries fact objects matching the given criteria.
        """
        q = super(TripleManager, self).get_query_set()
    
        #TODO:add support for nested/recursive searches, where the subject/predicate/object are query patterns?
        #q = type(self).objects.all()
        if subject:
            if isinstance(subject, models.Model):
                q = q.filter(_subject_id=subject.id, _subject_type=ContentType.objects.get_for_model(type(subject)))
            else:
                q = q.filter(_subject_text=str(subject))
        if predicate:
            if isinstance(predicate, models.Model):
                q = q.filter(_predicate_id=predicate.id, _predicate_type=ContentType.objects.get_for_model(type(predicate)))
            else:
                q = q.filter(_predicate_text=str(predicate))
        if object:
            if isinstance(object, models.Model):
                q = q.filter(_object_id=object.id, _object_type=ContentType.objects.get_for_model(type(object)))
            else:
                q = q.filter(_object_text=str(object))
        if gid:
            if not isinstance(gid, GraphId):
                q = q.filter(gid__value=str(gid))
            elif isinstance(gid, GraphId):
                q = q.filter(gid=gid)
            else:
                assert Exception, "Unknown graph ID value: %s" % gid
            
        return q
    
    def searchall(self,
                  args,
                  table_count=0,
                  q=None,
                  parent_field=None,
                  key_map=None,
                  unique_tables=None,
                  junk_count=0,
                  parent_var_name=None,
                  select_values=None,
                  parent_table_count=None,
                  lookup_objects=True,
                  limit=None,
                  single_name=None):
        """
        Allows searching for triples arranged in complex patterns.
        
        Parameters:
        
            args := A nested combination of lists and dictionaries definining
                the search pattern.
        
            q := Parent query to append new queries onto. Mainly used internally.
        
            limit := Equivalent to the Django ORM's list indexing operator.
                e.g. q[0] or q[2:4]
        
            single_name := If given, instead of returning a dictionary of key:value pairs,
                it will return only the value whose key equals <single_name>.
        """
        #TODO:track and enforce variable values, e.g. if the variable ?subject
        #occurs twice, create a WHERE clause that ensure both instances have the same value
        
        if select_values is None:
            select_values = []
        
        if unique_tables is None:
            unique_tables = set()
            
        local_unique_tables = set()
        
        # This is the starting table count at the current recursion depth, and
        # must not be incremented.
        local_table_count = table_count
        
        def get_parent_table_name():
            """
            Returns the name of the parent table being referred to in this
            call.
            """
            if local_table_count == 0:
                return 'triple_triple'
            elif local_table_count > 0:
                return 't%i' % local_table_count
            return
        
        def get_grandparent_table_name():
            """
            Returns the name of the parent table being referred to in this
            call.
            """
            if parent_table_count == 0:
                return 'triple_triple'
            elif parent_table_count > 0:
                return 't%i' % parent_table_count
            return
            
        def get_current_table_name():
            """
            Returns the name of the table currently being filtered
            in the current call.
            
            Note, table_count == 1 => table_name == 'triple_triple',
            whereas table_count == 2 => table_name == 't1'.
            
            This is because Django's ORM automatically inserts the first
            'triple_triple', regardless of what you pass .extra(),
            so this is treated as though it's aliased as t0.
            """
#            if local_table_count:
#                return 't%i' % (table_count-1)
#            else:
#                return 'triple_triple'
            if table_count > 1:
                return 't%i' % (table_count-1)
            else:
                return 'triple_triple'
        
        def get_table_as_name(table_name):
            """
            Returns the expression to include the table name
            in the tables=[] list passed to .extra().
            """
            if table_name == 'triple_triple':
                return table_name
            return '"triple_triple" AS "%s"' % table_name
        
        def get_tables():
            """
            Returns the list of tables to use for the current call to .extra().
            This makes sure that same table isn't added more than once, since
            .extra() doesn't check for duplicates, allowing the possibilities
            that you'll join the same aliased table, resulting in a ambiguity
            SQL error.
            
            Since the Triple model supports both simple text attributes, as
            well as references to arbitrary external Django models,
            this attempts to disambiguate which is being referenced.
            """
            tables = None
            table_name = get_table_as_name(get_current_table_name())
            if table_name not in unique_tables:
                tables = [table_name]
            unique_tables.add(table_name)
            #local_unique_tables.add(table_name)
            return tables
        
        if key_map is None:
            # A list of table fields that should all be equivalent.
            key_map = defaultdict(set) # {name:[table.field]}
        
        if q is None:
            q = super(TripleManager, self).get_query_set()
            
        for arg in args:
            assert isinstance(arg,dict), "Argument must be a dictionary."
            
            # Indicate that each element potentially joins a separate
            # set of records.
            table_count += 1
            
            for k,v in arg.iteritems():
                
                assert isinstance(k,basestring), "Invalid key: %s" % (k,)
                
                if isinstance(v,basestring) and v.startswith('?'):
                    if k in (S, P, O):#'subject','predicate','object'):
                        # Translate property name into model field name.
                        #TODO:Use CASE statements to distinguish between simple text and complex objects?
                        #TODO:Might have to use a wrapper around the query result to lookup the object.
                        tables = get_tables()#?
                        _select = {v[1:]+'_text':'%s._%s_text'%(get_current_table_name(),k),
                                   #v+'_object':'%s._%s_object'%(get_current_table_name(),k),
                                   v[1:]+'_id':'%s._%s_id'%(get_current_table_name(),k),
                                   v[1:]+'_type_id':'%s._%s_type_id'%(get_current_table_name(),k)}
                        for __k,__v in _select.iteritems(): key_map[__k].add(__v)
                        q = q.extra(select=_select, tables=tables)
                    else:
                        tables = get_tables()#?
                        _select = {v[1:]:'%s.%s'%(get_current_table_name(),k)}
                        for __k,__v in _select.iteritems(): key_map[__k].add(__v)
                        q = q.extra(select=_select, tables=tables)
                    select_values.extend(_select.keys())
                    continue
                
                # Extract variable name from key.
                # These variables will make up the SELECT columns in the
                # resulting query.
                var_name = None
                matches = re.findall("\?([a-zA-Z0-9_]+)", k)
                if matches:
                    var_name = matches[0]
                    k = k.replace('?'+var_name, '')
                
                if k in (S, P, O):#'subject','predicate','object'):
                    if isinstance(v, models.Model):
                        # Translate references to external models to the Triple's
                        # generic foreign key fields.
                        
                        tables = get_tables()
                        
                        q = q.extra(select={},
                                    tables=tables,
                                    where=["%s._%s_id = '%s'"%(get_current_table_name(),k,v.id),
                                           "%s._%s_type_id = %i"%(get_current_table_name(),k,ContentType.objects.get_for_model(type(v)).id)])
                        
                        
                        
                        # If this segment contains a variable name, then add it to
                        # the list of SELECT variables.
                        if var_name:
                            _select = {var_name:'%s._%s_id'%(get_current_table_name(),k)}
                            for __k,__v in _select.iteritems(): key_map[__k].add(__v)
                            q = q.extra(select=_select)
                            select_values.append(var_name)
                            
                    elif isinstance(v,list):
                        # Query using a nested expression where the parent's
                        # field has some relation to a child's subject field.
                        q = self.searchall(args=v,
                                       table_count=table_count,
                                       q=q,
                                       parent_field=k,
                                       key_map=key_map,
                                       junk_count=junk_count,
                                       unique_tables=unique_tables,
                                       select_values=select_values,
                                       parent_table_count=local_table_count,
                                       parent_var_name=var_name)
                    else:
                        # Query using a simple string field.
                        
                        tn = get_table_as_name(get_current_table_name())
                        tables = []
                        if tn not in local_unique_tables:# and tn not in unique_tables:
                            tables.append(tn)
                            local_unique_tables.update(tables)
                            #unique_tables.update(tables)
                            
                        junk_count += 1
                        _select = {'_j%i'%junk_count:'%s.id'%get_current_table_name()}
                        for __k,__v in _select.iteritems(): key_map[__k].add(__v)
                        q = q.extra(select=_select,
                                    tables=tables,
                                    where=["%s._%s_text = '%s'"%(get_current_table_name(),k,str(v))])
                        
                        # If this call is a subquery inside a larger query,
                        # then link the parent field to this child subject.
                        # e.g. parent_table_name._field_text = this_table_name._subject_text
                        if parent_table_count is not None and parent_field:
                            junk_count += 1
                            
                            tn = get_table_as_name(get_grandparent_table_name())
                            tables = []
                            if tn not in local_unique_tables:
                                tables.append(tn)
                                local_unique_tables.update(tables)
                            
                            _select = {'_j%i'%junk_count:'%s.id'%get_current_table_name()}
                            if parent_var_name:
                                _select[parent_var_name] = "%s._%s_text" % (get_grandparent_table_name(),parent_field,)
                                select_values.append(parent_var_name)
                            for __k,__v in _select.iteritems(): key_map[__k].add(__v)
                            q = q.extra(select=_select,
                                        tables=None,
                                        where=["%s._%s_text = %s._subject_text" % (get_grandparent_table_name(),parent_field,get_current_table_name(),)])
                        
                        # If this segment contains a variable name, then add it to
                        # the list of SELECT variables.
                        if var_name:
                            _select = {var_name:'%s._%s_text'%(get_current_table_name(),k)}
                            for __k,__v in _select.iteritems(): key_map[__k].add(__v)
                            q = q.extra(select=_select)
                            select_values.append(var_name)
                else:
                    # Query using a non subject/predicate/object field.
                    # e.g. uuid, _object_id, etc.
                    
                    tables = get_tables()
                    q = q.extra(select={},
                                tables=tables,
                                where=["%s.%s = %s"%(get_current_table_name(),k,repr(v),)])
                    
                    if var_name:
                        _select = {var_name:'%s.%s'%(get_current_table_name(),k)}
                        for __k,__v in _select.iteritems(): key_map[__k].add(__v)
                        q = q.extra(select=_select)
                        select_values.append(var_name)
        
        if select_values:
            q = q.values(*[k for k in select_values])
            
        if not local_table_count:
            # Perform actions at the end of the recursion.
            
            # Declare column equivalencies, as implied by any duplicate
            # variable names used in the query.
            # e.g. If ?id was bound to t1._object_id as well as
            # t2._predicate_id, then we'll add to the WHERE clause
            # t1._object_id = t2._predicate_id.
            _where = []
            for k,v in key_map.iteritems():
            
                # Skip fake column names.
                if k.startswith('_'):
                    continue
                
                # Skip names with less than two associated column names,
                # since we can't form an equivalency.
                if len(v) < 2:
                    continue
                
                _values = list(v)
                _last = _values[0]
                for _v in _values[1:]:
                    _where.append("%s = %s" % (_last, _v))
                    _last = v
            if _where:
                q = q.extra(select={}, where=_where)
                
            # Workaround bug in django/db/models/sql/query.py, that allows
            # duplicate table names, by stripping out the duplicates ourselves.
            q.query.extra_tables = tuple(remove_duplicates(q.query.extra_tables))
            
            class _GenericForeignKeyLookupWrapper:
                """
                Wraps a QuerySet and detects and looks up the model instance corresponding
                to the content type id and object id pairs for each row in the query.
                """
                def __init__(self, q, single_name=None):
                    self.q = q
                    self.single_name = single_name
                    
                @property
                def query(self):
                    return self.q.query
                    
                def count(self):
                    return self.q.count()
                
                def __getitem__(self, index):
                    if not isinstance(index, int):
                        return NotImplemented
                    i = 0
                    for row in iter(self):
                        i += 1
                        if i == index:
                            return row
                        
                def __iter__(self):
                    for row in self.q:
                        # Lookup GenericForeignKey objects.
                        _types = []
                        for _k in row.keys():
                            if _k.endswith('_type_id') and _k.replace('_type_id','_id') in row:
                                if _k.replace('_type_id','_text') in row:
                                    if row[_k.replace('_type_id','_text')] is None:
                                        # Dynamic property is a GenericForeignKey, so remove simple text field.
                                        type_id = row[_k]
                                        object_id = row[_k.replace('_type_id','_id')]
                                        row[_k.replace('_type_id','')] = ContentType.objects.get(id=type_id).model_class().objects.get(id=object_id)
                                        del row[_k]
                                        del row[_k.replace('_type_id','_id')]
                                        del row[_k.replace('_type_id','_text')]
                                    else:
                                        # Dynamic property is simple text, so remove GenericForeignKey fields.
                                        del row[_k]
                                        del row[_k.replace('_type_id','_id')]
                                        row[_k.replace('_type_id','')] = row[_k.replace('_type_id','_text')]
                                        del row[_k.replace('_type_id','_text')]
                                elif row[_k] and row[_k.replace('_type_id','_id')]:
                                    # If only the object_id and object_type_id exist, then lookup the object.
                                    type_id = row[_k]
                                    object_id = row[_k.replace('_type_id','_id')]
                                    row[_k.replace('_type_id','')] = ContentType.objects.get(id=type_id).model_class().objects.get(id=object_id)
                                    del row[_k]
                                    del row[_k.replace('_type_id','_id')]
                        if self.single_name:
                            yield row[single_name]
                        else:
                            yield row
            if lookup_objects:
                q = _GenericForeignKeyLookupWrapper(q, single_name=single_name)
        
        if limit is not None:
            if isinstance(limit, int):
                if limit > 0 and limit <= q.count():
                    q = q[limit]
                else:
                    q = None
            elif type(limit) in (tuple,list) and len(limit) == 2 and isinstance(limit[0],int) and isinstance(limit[1],int):
                q = q[limit[0]:limit[1]]
                
        return q

def expand_triple_argument(arg, gid=None, triples=None):
    """
    If the argument represents shorthand notation for a triple network,
    then it creates the triple network, instantiating a UUID to use as
    the common subject value.
    
    Otherwise, passes the value through directly.
    
    e.g.
    
    uuid = uuid.uuid4()
    Triple('I','replied',uuid).save()
    Triple(uuid,'to','#sentence1').save()
    Triple(uuid,'with','#response3').save()
    
    is equivalent to:
    
    Triple('I','replied',[('to','#sentence1'),('with','#response3')]).save()
    
    or using dictionary notation:
    
    Triple('I','replied',{'to':'#sentence1','with':'#response3'}).save()
    """
    if isinstance(arg, dict):
        arg = arg.iteritems()
    elif type(arg) not in (tuple,list):
        return arg
    id = '#'+str(uuid.uuid4())
    for el in arg:
        assert len(el) == 2, "Invalid element '%s' in argument. All elements must 2-item tuples or lists." % (el,)
        predicate,object = el
        t = T(id, predicate, object, gid=gid, triples=triples)
    return id

def T(subject=None, predicate=None, object=None, gid=None, triples=None):
    """
    This is a convenience method for making Triples.
    """
    if gid:
        if type(gid) not in (tuple,list):
            gid = [gid]
        _gid = gid
        gid = []
        for el in _gid:
            if not isinstance(el, GraphId):
                el,_ = GraphId.objects.get_or_create(value=str(el))
            gid.append(el)
    subject = expand_triple_argument(subject, gid=gid, triples=triples)
    predicate = expand_triple_argument(predicate, gid=gid, triples=triples)
    object = expand_triple_argument(object, gid=gid, triples=triples)
    if subject and predicate is None and object is None:
        return subject
    t = Triple()
    t.subject = subject
    t.predicate = predicate
    t.object = object
    t.save()
    if gid:
        for el in gid:
            t.graphs.add(el)
    if triples is not None:
        triples.append(t)
    return t

class GraphId(_BaseModel):
    value = models.CharField(max_length=300, blank=False, null=False, db_index=True, unique=True)
    
    def __str__(self):
        return self.value

def GID(value):
    return GraphId.objects.get_or_create(value=value)[0]

class Triple(models.Model):#_BaseModel):
    #gid = models.CharField(max_length=100, blank=True, null=True, db_index=True)
    #gid = models.ForeignKey(GraphId, blank=True, null=True)
    uuid = UUIDField(blank=False, null=False, db_index=True, unique=True)
    #creation_timestamp = models.DateTimeField(auto_now_add=True, blank=False, null=False, db_index=True)
    #last_check_timestamp = models.DateTimeField(auto_now_add=True, blank=False, null=False, db_index=True)
    
    _subject_type = models.ForeignKey(ContentType, related_name='fact_subject_type', blank=True, null=True, db_index=True)
    _subject_id = models.PositiveIntegerField(blank=True, null=True, db_index=True)
    _subject_object = generic.GenericForeignKey('_subject_type', '_subject_id')
    _subject_text = models.CharField(max_length=MAX_LENGTH, blank=True, null=True, db_index=True)
    
    _predicate_type = models.ForeignKey(ContentType, related_name='fact_predicate_type', blank=True, null=True, db_index=True)
    _predicate_id = models.PositiveIntegerField(blank=True, null=True, db_index=True)
    _predicate_object = generic.GenericForeignKey('_predicate_type', '_predicate_id')
    _predicate_text = models.CharField(max_length=MAX_LENGTH, blank=True, null=True, db_index=True)
    
    _object_type = models.ForeignKey(ContentType, related_name='fact_object_type', blank=True, null=True, db_index=True)
    _object_id = models.PositiveIntegerField(blank=True, null=True, db_index=True)
    _object_object = generic.GenericForeignKey('_object_type', '_object_id')
    _object_text = models.CharField(max_length=MAX_LENGTH, blank=True, null=True, db_index=True)
    
    objects = TripleManager()
    
    graphs = models.ManyToManyField(GraphId)
    
    def copy(self):
        t = Triple(_subject_type=self._subject_type, _subject_id=self._subject_id, _subject_text=self._subject_text,
                   _predicate_type=self._predicate_type, _predicate_id=self._predicate_id, _predicate_text=self._predicate_text,
                   _object_type=self._object_type, _object_id=self._object_id, _object_text=self._object_text)
        t.save()
        for gid in self.graphs.all():
            t.graphs.add(gid)
        return t
    
    @classmethod
    def current_object(cls):
        q = cls.objects.raw("""
SELECT t1.*
FROM triple_triple AS t1, (
    SELECT
        _subject_text,
        _subject_id,
        _subject_type_id,
        _predicate_text,
        _predicate_id,
        _predicate_type_id,
        MAX(creation_timestamp) AS creation_timestamp
    FROM triple_triple
    GROUP BY
        _subject_text,
        _subject_id,
        _subject_type_id,
        _predicate_text,
        _predicate_id,
        _predicate_type_id
) AS t2
WHERE   1 = 1
    AND (
            (t1._subject_id IS NULL AND t1._subject_text = t2._subject_text)
            OR
            (t1._subject_text IS NULL AND t1._subject_id = t2._subject_id)
    )
    AND (
            (t1._predicate_id IS NULL AND t1._predicate_text = t2._predicate_text)
            OR
            (t1._predicate_text IS NULL AND t1._predicate_id = t2._predicate_id)
    )
    AND t1.creation_timestamp = t2.creation_timestamp;
        """)
        return q
    
    @property
    def subject(self):
        if self._subject_object:
            return self._subject_object
        return self._subject_text
    
    @subject.setter
    def subject(self, data):
        if isinstance(data, models.Model):
            self._subject_text = None
            self._subject_id = data.id
            self._subject_type = ContentType.objects.get_for_model(type(data))
        else:
            self._subject_id = None
            self._subject_type = None
            self._subject_text = str(data)
        return data
    
    @property
    def predicate(self):
        if self._predicate_object:
            return self._predicate_object
        return self._predicate_text
    
    @predicate.setter
    def predicate(self, data):
        if isinstance(data, models.Model):
            self._predicate_text = None
            self._predicate_id = data.id
            self._predicate_type = ContentType.objects.get_for_model(type(data))
        else:
            self._predicate_id = None
            self._predicate_type = None
            self._predicate_text = str(data)
        return data
    
    @property
    def object(self):
        if self._object_object:
            return self._object_object
        return self._object_text
    
    @object.setter
    def object(self, data):
        if isinstance(data, models.Model):
            self._object_text = None
            self._object_id = data.id
            self._object_type = ContentType.objects.get_for_model(type(data))
        else:
            self._object_id = None
            self._object_type = None
            self._object_text = str(data)
        return data
    
    def __repr__(self):
        return "<Triple:%i %s, %s, %s>" % (self.id, self.subject, self.predicate, self.object,)
    
    def __str__(self):
        return repr(self)
    
    def match(cls, subject=None, predicate=None, object=None):
        """
        Queries fact objects matching the given criteria.
        """
        #TODO:add support for nested/recursive searches, where the subject/predicate/object are query patterns?
        q = type(self).objects.all()
        if subject:
            if isinstance(subject, models.Model):
                q = q.filter(_subject_id=subject.id, _subject_type=ContentType.objects.get_for_model(type(_subject)))
            else:
                q = q.filter(_subject_text=str(subject))
        if predicate:
            if isinstance(predicate, models.Model):
                q = q.filter(_predicate_id=predicate.id, _predicate_type=ContentType.objects.get_for_model(type(_predicate)))
            else:
                q = q.filter(_predicate_text=str(predicate))
        if object:
            if isinstance(object, models.Model):
                q = q.filter(_object_id=object.id, _object_type=ContentType.objects.get_for_model(type(_object)))
            else:
                q = q.filter(_object_text=str(object))
        return q
    
    class Meta:
        pass

TS = Triple.objects.searchall

d = D = dict

class Literal(object):
    def __init__(self, value):
        self.value = value
        self.id_var_name = None
        if ':id=?' in self.value:
            matches = re.findall(":id=\?[a-zA-Z0-9_]+", self.value)
            for match in matches:
                self.id_var_name = match[5:]
                self.value = self.value.replace(match, '')
        
    @property
    def is_variable(self):
        return isinstance(self.value, basestring) and self.value.startswith('?')
        
    def __hash__(self):
        return hash(self.value)
    
    def __eq__(self, other):
        if not isinstance(other, Literal):
            return NotImplemented
        return other.value == self.value
    
    def __repr__(self):
        return '%s(%s)' % (type(self).__name__, repr(self.value))
    
    def sql(self, depth=0):
        return "'%s'" % self.value

class Thing(object):
    """
    Represents the concept of a symbolic "thing" with associated attributes and
    values.
    e.g. the "[]" in the Notation3 statement "[] isa apple."
    """
    def __init__(self, subject=None, **kwargs):
        self.first_alias = None
        self.subject = None
        if subject is not None and subject != ANY:
            self.subject = Literal(subject)
        self.items = {}
        for k,v in kwargs.iteritems():
            assert isinstance(k, basestring)
            if isinstance(v, models.Model) or isinstance(v, basestring) or type(v) in (bool,int,float):
                v = Literal(v)
            else:
                assert isinstance(v, dict)
                v = Thing(**v)
            self.items[Literal(k)] = v
    
    def sql(self, depth=0, table_count=0, parent_table_prefix='t', parent_column=None, variable_map=None, parent_table_alias=None):
        """
        Generates a SQL SELECT query corresponding to N3 structure.
        
        Parameters:
            depth := The recursion depth of the current call.
            table_count := An integer of the number of tables joined at the current recursion depth.
            parent_table_prefix := A string used as the prefix for the current table alias.
            parent_column := The SQL column that is equivalent to the current _object_text.
            variable_map := An index listing all the variables bound to each column.
        """
        
        def _make_table_alias():
            table_alias = "%s%i" % (parent_table_prefix,len(tables)+table_count)
            #table_alias = "%s%i" % (parent_table_prefix,table_count)
            if self.first_alias is None:
                self.first_alias = table_alias
            local_aliases.append(table_alias)
            tables.add('triple_triple AS %s' % table_alias)
            return table_alias
        
        select = []
        tables = set()
        where = []
        #first_alias = None
        local_aliases = []
        if variable_map is None:
            variable_map = defaultdict(set) # {variable_name:set([column names])}
        for k,v in self.items.iteritems():
            
            # Track query of special internal columns.
            if isinstance(k,Literal) and k.value.startswith('_'):
                assert isinstance(v,Literal), "Non-literal equivalency to internal columns not supported: %s" % (v,)
                assert v.is_variable#todo:support non-variables?
                
                internal_column_name = k.value[1:]
                if self.first_alias:
                    variable_map[v.value].add("%s.%s" % (self.first_alias,internal_column_name))
                else:
                    # Add alias later in _build_sql().
                    variable_map[v.value].add("%(first_alias)s."+internal_column_name)
                continue
                
            # Add table name for FROM clause.
            table_alias = _make_table_alias()
            
            # Add subject to WHERE clause.
            if parent_column:
                # Link the current triple to a clause in a container
                # triple.
                # e.g. Given the statement "[] has [attr value]" this
                # would bind the ?object in the triple "[] has ?object"
                # to the ?subject in "?subject attr value".
                where.append("%s = %s._subject_text" % (parent_column,table_alias))
            if len(local_aliases) > 1:
                # If we've generated multiple tables all using the same subject,
                # then we need to link all their subjects together.
                where.append("%s._subject_text = %s._subject_text" % (local_aliases[-2],table_alias))
            if self.subject:
                # Add the literal subject, if specified.
                assert isinstance(self.subject, Literal), "Non-literal subjects not supported."#todo:?
                if isinstance(self.subject.value, models.Model):
                    where.append("%s._subject_id = %s" % (table_alias, self.subject.value.id))
                    where.append("%s._subject_type_id = %s" % (table_alias, ContentType.objects.get_for_model(type(self.subject.value)).id))
                elif self.subject.is_variable:
                    variable_map[self.subject.value].add("%s._subject_text" % table_alias)
                else:
                    assert isinstance(self.subject.value, basestring)
                    where.append("%s._subject_text = %s" % (table_alias, self.subject.sql()))
            
            # Add predicate to WHERE clause.
            assert isinstance(k, Literal), "Non-literal keys not supported."#todo:?
            if k.value != ANY:
                where.append("%s._predicate_text = %s" % (table_alias, k.sql()))
            #todo:support models.Model predicates?
            
            # Add object to WHERE clause.
            if isinstance(v, Literal):
                if v.id_var_name:
                    variable_map['?'+v.id_var_name].add("%s.id" % table_alias)
                    
                if isinstance(v.value, models.Model):
                    where.append("%s._object_id = %s" % (table_alias, v.value.id))
                    where.append("%s._object_type_id = %s" % (table_alias, ContentType.objects.get_for_model(type(v.value)).id))
                elif v.is_variable:
                    variable_map[v.value].add("%s._object_text" % table_alias)
                else:
                    where.append("%s._object_text = %s" % (table_alias, v.sql()))
            else:
                assert isinstance(v, Thing)
                _select,_tables,_where = v.sql(depth=depth+1,
                                       #table_count=table_count,
                                       table_count=len(tables)+table_count,
                                       variable_map=variable_map,
                                       parent_table_alias=table_alias,
                                       parent_column='%s._object_text'%(table_alias,),
                                       parent_table_prefix=table_alias+'_')
                select.extend(_select)
                tables.update(_tables)
                where.extend(_where)
        
            # Prepare next iteration.
            #table_count += 1
        
        # Generate final SQL if we're at the end of the top-level call.
        # Otherwise, just return data structures to merge with higher
        # the level.
        if depth == 0:
            return _build_sql(select, tables, where, variable_map, order_by=None, limit=None)
        else:
            return select,tables,where

class Query(object):
    """
    Represents the top-level object containing a triple query.
    """
    
    def __init__(self, where=None, select=None, constraints=None, order_by=None, limit=None, same_graph=True, *args, **kwargs):
        self.select = select
        self.constraints = [] if constraints is None else constraints
        self.order_by = order_by or []
        self.limit = limit
        self.things = []
        self.same_graph = same_graph
        if where is None:
            assert kwargs, "Either where or kwargs must be specified"
            where = kwargs
        if where is not None:
            if isinstance(where, dict):
                for k,v in where.iteritems():
                    assert isinstance(v, D)
                    self.things.append(Thing(subject=k, **v))
            else:
                assert isinstance(where, list), "Where argument must be a dictionary or list of (key,value) tuples."
                for item in where:
                    if isinstance(item,basestring):
                        self.constraints.append(item)
                    else:
                        k,v = item
                        assert isinstance(v, D)
                        self.things.append(Thing(subject=k, **v))
        
    def sql(self, depth=0, variable_map=None):
        select = list(self.select or [])
        tables = set()
        where = []
        if variable_map is None:
            variable_map = defaultdict(set) # {variable_name:set([column names])}
            
        # Collect SQL parts from each sub-component.
        for thing in self.things:
            _select,_tables,_where = thing.sql(depth=depth+1,
                                               table_count=len(tables),
                                               variable_map=variable_map,)
            select.extend(_select)
            tables.update(_tables)
            where.extend(_where)
            
        # Assert that all the matching triples must be tagged within at least one common graph.
        if self.same_graph:
            last_graph_alias = None
            for i,table in enumerate(list(tables)):
                table_alias = table[table.index(' AS ')+4:]
                graph_alias = "%s_g%i" % (table_alias, i)
                tables.add("triple_triple_graphs AS %s" % graph_alias)
                where.append("%s.id = %s.triple_id" % (table_alias,graph_alias))
                if last_graph_alias:
                    where.append("%s.graphid_id = %s.graphid_id" % (graph_alias,last_graph_alias))
                last_graph_alias = graph_alias
            
        # Convert variable names in constraints into SQL column names and add
        # them to the WHERE clause.
        if self.constraints:
            for constraint in self.constraints:
                var_names = re.findall("\?[a-zA-Z0-9_]+", constraint)
                for var_name in var_names:
                    assert var_name in variable_map, "Unknown constraint variable: %s" % var_name
                    constraint = constraint.replace(var_name, sorted(variable_map[var_name])[0])
            where.append(constraint)
            
        # Construct order by list.
        order_by = []
        if self.order_by:
            for v in self.order_by:
                flip = v.startswith('-')
                if flip: v = v[1:]
                if v.startswith('?'):
                    # Convert variable name to column name.
                    v = sorted(variable_map[v])[0]
                if flip: v = '-'+v
                order_by.append(v)
        
        # Validate limit.
        limit = self.limit
        assert limit is None or isinstance(limit, int)
            
        if depth == 0:
            return _build_sql(select, tables, where, variable_map, order_by, limit)
        else:
            return select,tables,where
        
    def execute(self):
        cursor = getDictCursor()
        cursor.execute(self.sql())
        return cursor

def _build_sql(select, tables, where, variable_map, order_by, limit):
    """
    Helper function to join SQL list parts into final SQL query string.
    """
    # Add column constraints implied by duplicate variable name
    # usage.
    for _variable_name,_columns in variable_map.iteritems():
        _last = None
        for _column in sorted(_columns):
            if _last:
                where.append("%s = %s" % (_last,_column))
            _last = _column
    
    # Validate SELECT columns.
    select_str = []
    for sel_name in select:
        assert sel_name in variable_map, "Unable to select '%s' because it's not bound to any column. Did you forget to use this in your query?" % (sel_name,)
        col_name = sorted(variable_map[sel_name])[0]
        select_str.append("%s AS %s" % (col_name, sel_name[1:]))
    
    if select_str:
        select_str = 'SELECT  '+(',\n        '.join(sorted(select_str)))
    else:
        select_str = ''
    table_aliases = sorted([t.split(' AS ')[-1] for t in tables])
    select_str = select_str % dict(first_alias=table_aliases[0])
    
    from_str = 'FROM    '+(',\n        '.join(sorted(tables)))
    where_str = 'WHERE   '+('\n    AND '.join(sorted(where)))
    
    order_by = order_by or []
    order_by_str = ('ORDER BY\n        ' + ',\n        '.join(order_by)) if order_by else ''
    
    limit_str = "LIMIT %i" % limit if isinstance(limit,int) and limit > 0 else ''
    
    return (select_str + '\n' + from_str + '\n' + where_str + '\n' + order_by_str + '\n' + limit_str).strip()
