import os, sys, random, time
import cPickle as pickle
from pprint import pprint
from collections import defaultdict

from django.test import TestCase
from django.utils import unittest
from django.db import IntegrityError, transaction, connection
from django.db.models.query_utils import Q
from django.contrib.contenttypes.models import ContentType
from django.conf import settings

from triple import models
from triple.models import Triple, T, TS, ANY, Query

class Test(TestCase):
    
    def test_entity_creation(self):
        """
        Confirm the helper triple creator T()
        returns the entity UUID when used to create multiple triples.
        """
        id = T([
            ('is', [('#hardware', '#drive')]),
            ('has', [('#mountpoint', '/dev/sda')]),
        ], gid='#graph1')
        q = list(Triple.objects.all())
        self.assertEqual(len(q), 4)
        self.assertTrue(isinstance(id, basestring))
        
#        id = T([
#            ('is', [('#hardware', '#drive')]),
#            ('has', [('#mountpoint', '/dev/sdb')]),
#        ], gid='#graph2')
        id = T({
            'is':{'#hardware':'#drive'},
            'has':{'#mountpoint':'/dev/sdb'},
        }, gid='#graph2')
        q = list(Triple.objects.all())
        self.assertEqual(len(q), 8)
        self.assertTrue(isinstance(id, basestring))
        
        # Query triples in a specific graph.
#        g2 = models.GraphId.objects.get(value='#graph2')
#        q = TS([
#                  {'id':'?id',
#                   },
#                   {'_subject_id':'?id',
#                    '_subject_type_id':ContentType.objects.get_for_model(Triple).id,
#                    'predicate':models.BELONGSTOGRAPH,
#                    'object?graph_id':g2},
#                  ], lookup_objects=0)
#        q = list(q)
#        self.assertEqual(len(q), 4)
    
    def test_basics(self):
        """
        Confirm basic ability to create and query triples.
        """
        f1 = T('#bob','has-a','hat')
        self.assertTrue(f1)
        self.assertEqual(f1.subject, '#bob')
        self.assertEqual(f1.predicate, 'has-a')
        self.assertEqual(f1.object, 'hat')
        T('#bob','has-a','cat')
        T('#bob','has-a','rat')
        T('#bob','knows','#drew')
        
        T('#sue','has-a','mat')
        T('#sue','has-a','house')
        T('#sue','knows','#drew')
        
        T('#drew','knows','#sue')
        T('#drew','knows','#bob')
        
        q = models.Triple.objects.search(subject='#sue')
        #print q
        self.assertEqual(q.count(), 3)
        
        # Query triple belonging to a specific graph.
        f1 = T('#drew','knows','#bob')
        g1,_ = models.GraphId.objects.get_or_create(value='#graphs/people')
        f2 = T(f1, '#belongsTo', g1)
        #print f2
        
        q = TS([
                  {'id':'?id',
                   'subject':'#drew',
                   'object':'?object',
                   },
                   {'_subject_id':'?id',
                    '_subject_type_id':ContentType.objects.get_for_model(Triple).id,
                    'subject':'?fact',
                    'predicate':'#belongsTo',
                    'object?graph_id':g1},
                  ], lookup_objects=1)
        #print q.query
        q = list(q)
#        for row in q:
#            print row
        
        self.assertEqual(len(q), 1)
        self.assertEqual(q[0]['graph_id'], g1.id)
        self.assertEqual(q[0]['id'], f1.id)
        self.assertEqual(q[0]['object'], f1.object)
        self.assertEqual(q[0]['fact'], f1)
        
    def test_nested_creation(self):
        """
        Confirm connected triples can be created via shorthand.
        """
        
        ## I replied to ?sentence with response ?response.
        t = T('I','replied',[('to','#sentence1'),('with','#response3'),('on','#date3')])
        q = Triple.objects.all()
        self.assertEqual(len(q), 4)
        for t in q:
            print t
        