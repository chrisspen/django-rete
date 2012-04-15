# -*- coding: utf-8 -*-
from django.test import TestCase

from triple.models import Triple, T, GraphId
from triple.constants import *

import models
from constants import *

def walk_nodes(node, indent=0):
    print '    '*indent,type(node).__name__,node.id,node.field_name,node.operation_name,node.value
    print '    '*indent,'parent:',node.parent
    print '    '*indent,'items:',node.items.all().count()
    for t in node.items.all():
        print '    '*(indent+1),t
    print '    '*indent,'betajoins:',node.successors.all().count()
    for n in node.successors.all():
        print '    '*(indent+1),n
        
        print '    '*(indent+2),'pnodes:',n.pnodes.all().count()
        for pn in n.pnodes.all():
            print '    '*(indent+3),pn
            
        print '    '*(indent+2),'tests:',n.tests.all().count()
        for test in n.tests.all():
            print '    '*(indent+3),test
            
    for child in node.children.all():
        walk_nodes(child, indent+1)
    
class Test(TestCase):
    
    def test_add_wme(self):
        rete = models.Rete().save()
        
        p1 = models.Production.get('p1',[])
        
        c1 = ('?','?x','on','?y')
        c2 = ('?','?y','left-of','?z')
        c3 = ('?','?z','color','red')
        c4 = ('?','?a','color','maize')
        c5 = ('?','?b','color','blue')
        c6 = ('?','?c','color','green')
        c7 = ('?','?d','color','white')
        c8 = ('?','?s','on','table')
        c9 = ('?','?y','?a','?b')
        c10 = ('?','?a','left-of','?d')
        c11 = ('1','?i','right-of','?u')
        c12 = ('?','?i','height','<4')
        c13 = ('?','?i','height','>3')
        c14 = ('?','?i','height','≤2')
        c15 = ('?','?i','height','≥3')
        c16 = ('?','?i','height','≠3')
        conditions = [c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16]
        
        for condition in conditions:
            condition = models.Condition.get(p1, *condition)
            rete.build_or_share_alpha_memory(condition)
            
        nodes = models.AlphaNode.objects.filter(parent=None)
            
#        for n in nodes:
#            walk_nodes(n)
        
        facts = []
        facts.append(T('block1', 'on', 'block2'))
        facts.append(T('block2', 'left-of', 'block3'))
        facts.append(T('block3', 'color', 'red'))
        facts.append(T('block4', 'color', 'blue'))
        facts.append(T('block5', 'color', 'yellow'))
        self.assertEqual(len(facts), 5)
        for fact in facts:
            rete.add_wme(fact)
        self.assertEqual(rete.items.all().count(), len(facts))
        
        nodes = models.AlphaNode.objects.filter(parent=None)
        
    def test_remove_wme(self):
        rete = models.Rete().save()
        p1 = models.Production.get('p1',[
            ['?','?x','on','?y'],
            ['?','?y','left-of','?z'],
            ['?','?z','color','red'],
        ])
        rete.add_production(p1)
        self.assertEqual(len(list(rete.triggered_pnodes)), 0)
        
        facts = []
        facts.append(T('block1', 'on', 'block2'))
        facts.append(T('block2', 'left-of', 'block3'))
        facts.append(T('block3', 'color', 'red'))
        facts.append(T('block11', 'on', 'block21'))
        facts.append(T('block21', 'left-of', 'block31'))
        facts.append(T('block31', 'color', 'red'))
        
        for fact in facts:
            rete.add_wme(fact)
        self.assertEqual(rete.items.all().count(), len(facts))
        self.assertEqual(len(list(rete.triggered_pnodes)), 1)
        self.assertEqual(list(rete.triggered_pnodes)[0].triggered, 2)
        
        rete.remove_wme(facts[1])
        
        # Confirm production is still triggered, but that the trigger count
        # has been reduced by one.
        self.assertEqual(len(list(rete.triggered_pnodes)), 1)
        self.assertEqual(list(rete.triggered_pnodes)[0].triggered, 1)
        
        # Confirm production was un-triggered.
        rete.remove_wme(facts[3])
        self.assertEqual(len(list(rete.triggered_pnodes)), 0)
    
    def test_betanode_perform_join_tests(self):
        
        rete = models.Rete().save()
        
        # Represents the LHS of one production.
        conditions = []
        conditions.append(('?','?x','on','?y'))
        conditions.append(('?','?y','left-of','?z'))
        conditions.append(('?','?z','color','red'))
        
        facts = []
        facts.append(T('block1', 'on', 'block2'))
        facts.append(T('block2', 'left-of', 'block3'))
        facts.append(T('block3', 'color', 'red'))
        
        # Arbitrary alphanode, needed for right side of betanode.
        an = models.AlphaNode(rete=rete, field=models.ID_IDX, operation=models.EQ_IDX, value=None).save()
        bn = models.BetaJoinNode(alphanode=an, _alphanode=an).save()
        
        # Note, dummy token, not shown here, always has index == 0.
        t0 = models.Token(wme=facts[0], parent=None, index=1).save()
        self.assertEqual(t0.index, 1)
        t1 = models.Token(wme=facts[1], parent=t0, index=t0.index+1).save()
        self.assertEqual(t1.index, 2)
        t2 = models.Token(wme=facts[2], parent=t1, index=t1.index+1).save()
        self.assertEqual(t2.index, 3)
        
        # Confirm indexing order by tokens according to Token.get_wme(index)
        self.assertEqual(t2.get_wme(index=1), facts[0])
        self.assertEqual(t2.get_wme(index=2), facts[1])
        self.assertEqual(t2.get_wme(index=3), facts[2])
        
        test = models.TestAtJoinNode(parent=bn,
                              field_of_arg1=models.S_IDX,
                              condition_number_of_arg2=2,
                              field_of_arg2=models.O_IDX).save()
        self.assertEqual(bn.tests.all().count(), 1)
        #TODO:why facts[2]?
        self.assertEqual(bn.perform_join_tests(token=t2, wme=facts[2]), True)
        
    def test_wme_removal_structures(self):
        rete = models.Rete().save()
        
        # Represents the LHS of one production.
        conditions = []
        conditions.append(('?','?x','on','?y'))
        conditions.append(('?','?y','left-of','?z'))
        conditions.append(('?','?z','color','red'))
        
        facts = []
        facts.append(T('block1', 'on', 'block2'))
        facts.append(T('block2', 'left-of', 'block3'))
        facts.append(T('block3', 'color', 'red'))
        
        for i,condition in enumerate(conditions):
            p = models.Production.get('p%i'%i,[])
            condition = models.Condition.get(p, *condition)
            rete.build_or_share_alpha_memory(condition)
            
        for fact in facts:
            rete.add_wme(fact)
        
        # Per page 29, find all alphanode memories containing this WME.
        wme = facts[0]
        self.assertEqual(wme.alphanodes.all().count(), 2)
        
        # Per page 29, find all tokens with WME as the last element.
        # Create sample tokens.
        wme = facts[2]
        t0 = models.Token(wme=facts[0], parent=None).save()#1
        t1 = models.Token(wme=facts[1], parent=t0).save()
        t2 = models.Token(wme=facts[2], parent=t1).save()#3
        t0 = models.Token(wme=facts[2], parent=None).save()
        t1 = models.Token(wme=facts[0], parent=t0).save()
        t2 = models.Token(wme=facts[1], parent=t1).save()#6
        q = models.Token.objects.filter(wme=wme).exclude(id__in=models.Token.objects.filter(parent__isnull=False).values_list('parent'))
        self.assertEqual(q.count(), 1)
        self.assertEqual(q[0].id, 3)
        
    def test_get_join_tests_from_condition(self):
        rete = models.Rete().save()
        
        p1 = models.Production.get('p1',[])
        
        conditions = []
        conditions.append(('?','?x','on','?y'))
        conditions.append(('?','?y','left-of','?z'))
        conditions.append(('?','?z','color','red'))
        
        bc = {}
        bf = {}
        earlier_conds = []
        for i,condition in enumerate(conditions):
            cObj = models.Condition.get(p1, *condition)
            tests = rete.get_join_tests_from_condition(cObj, earlier_conds, bc, bf)
            earlier_conds.extend(tests)
            
        self.assertEqual(len(earlier_conds), 2)
        self.assertEqual(repr(earlier_conds[0]), "<TestAtJoinNode: parent=<NoneType:None>, wme.subject == token[1].wme.object>")
        self.assertEqual(repr(earlier_conds[1]), "<TestAtJoinNode: parent=<NoneType:None>, wme.subject == token[1].wme.object>") #TODO:fix?
    
    def test_condition(self):
        
        c = models.Condition.get(None, '?','?x','on','?y')
        self.assertEqual(list(c.constant_tests), [(P_IDX, EQ_IDX, u'on')])
        self.assertEqual(list(c.variable_bindings), [(S_IDX, 'x'),(O_IDX, 'y')])
        
        c = models.Condition.get(None, '?','?x','attr','≤12.45')
        self.assertEqual(list(c.constant_tests), [(P_IDX, EQ_IDX, u'attr'),(O_IDX, LE_IDX, u'12.45')])
        self.assertEqual(list(c.variable_bindings), [(S_IDX, 'x')])
    
    def test_add_production(self):
        rete = models.Rete().save()
        
        # Add production to RETE network.
        p1 = models.Production.get('rule1', [
            ['?','?x','on','?y'],
            ['?','?y','left-of','?z'],
            ['?','?z','color','red'],
        ])
        rete.add_production(p1)
        
        # Confirm correct top-alphanode creation.
        node0,_ = models.AlphaNode.objects.get_or_create(rete=rete,
                                                       field=None,
                                                       operation=None,
                                                       value=None,
                                                       parent=None)
        q = models.AlphaNode.objects.filter(rete=rete,
                                                       field=None,
                                                       operation=None,
                                                       value=None,
                                                       parent=None)
        self.assertEqual(q.count(), 1)
        self.assertEqual(q[0], node0)
        node1,_ = models.AlphaNode.objects.get_or_create(rete=rete,
                                                       field=None,
                                                       operation=None,
                                                       value=None,
                                                       parent=None)
        self.assertEqual(node0, node1)
        nodes = models.AlphaNode.objects.filter(rete=rete,
                                                       field__isnull=True,
                                                       operation__isnull=True,
                                                       value__isnull=True,
                                                       parent__isnull=True)
        self.assertEqual(nodes.count(), 1)
        
        # Validate data structures created by addition.
        nodes = models.AlphaNode.objects.filter(parent=None)
#        for n in nodes:
#            walk_nodes(n)
        self.assertEqual(models.AlphaNode.objects.all().count(), 5)
        self.assertEqual(models.BetaMemoryNode.objects.all().count(), 2)
        self.assertEqual(models.BetaJoinNode.objects.all().count(), 3)
        self.assertEqual(models.PNode.objects.all().count(), 1)
        pnode = models.PNode.objects.all()[0]
        self.assertTrue(pnode.parent)
        self.assertTrue(pnode in list(pnode.parent.children))
        
        # Define facts matching production 1.
        facts = []
        facts.append(T('block1', 'on', 'block2'))
        facts.append(T('block2', 'left-of', 'block3'))
        facts.append(T('block3', 'color', 'red'))
        f1,f2,f3 = facts
        f_b1_on_b2, f_b2_leftOf_b3, f_b3_color_red = facts
        
        # Add facts to RETE network as working memory elements.
        for fact in facts:
            rete.add_wme(fact)
            
        nodes = models.AlphaNode.objects.filter(parent=None)
#        if 0:
#            for n in nodes:
#                walk_nodes(n)
            
        # Confirm alphanode network structure.
        anodes = list(models.AlphaNode.objects.all().order_by('id'))
        self.assertEqual(len(anodes), 5)
        self.assertEqual(repr(anodes[0]), "<AlphaNode:1 None None None>")
        self.assertEqual(repr(anodes[1]), "<AlphaNode:2 predicate = on>")#?
        self.assertEqual(repr(anodes[2]), "<AlphaNode:3 predicate = left-of>")#?
        self.assertEqual(repr(anodes[3]), "<AlphaNode:4 predicate = color>")
        self.assertEqual(repr(anodes[4]), "<AlphaNode:5 object = red>")#?
        
        # Confirm betajoinnode/betamemorynode network structure.
        betamemory = list(models.BetaMemoryNode.objects.all().order_by('id'))
        betajoin = list(models.BetaJoinNode.objects.all().order_by('id'))
        self.assertEqual(len(betamemory), 2)
        self.assertEqual(len(betajoin), 3)
        self.assertEqual(betajoin[0].parent, None)
        self.assertEqual(betajoin[0].child, betamemory[0])
        self.assertEqual(betajoin[0].alphanode, anodes[1])#?
        self.assertEqual(betamemory[0].parent, betajoin[0])
        self.assertEqual(list(betamemory[0].children.all()), [betajoin[1]])
        self.assertEqual(betajoin[1].parent, betamemory[0])
        self.assertEqual(betajoin[1].child, betamemory[1])
        self.assertEqual(betajoin[1]._alphanode, anodes[2])#?
        self.assertEqual(betamemory[1].parent, betajoin[1])
        self.assertEqual(list(betamemory[1].children.all()), [betajoin[2]])
        self.assertEqual(betajoin[2].parent, betamemory[1])
        self.assertEqual(betajoin[2].child, None)
        self.assertEqual(betajoin[2]._alphanode, anodes[4])#?
        
        # Confirm alphanode memory contents.
        self.assertEqual(list(anodes[0].items.all()), [f1,f2,f3])
        self.assertEqual(list(anodes[1].items.all()), [f1])
        self.assertEqual(list(anodes[2].items.all()), [f2])
        self.assertEqual(list(anodes[3].items.all()), [f3])
        self.assertEqual(list(anodes[4].items.all()), [f3])
        
        # Confirm betamemory.
        self.assertEqual(len(betamemory), 2)
        toks1 = list(betamemory[0].tokens.all())
        self.assertEqual(len(toks1), 1)
        self.assertEqual(toks1[0].wme, f_b1_on_b2)
        toks2 = list(betamemory[1].tokens.all())
        self.assertEqual(len(toks2), 1)
        self.assertEqual(toks2[0].wme, f_b2_leftOf_b3)
        self.assertEqual(toks2[0].parent, toks1[0])
        
        # Confirm betajoinnode tests.
        # There should be exactly 2 tests, one for each duplicate reference of
        # a variable among conditions.
        tests = list(models.TestAtJoinNode.objects.all())
        self.assertEqual(len(tests), 2) # One match per WME+condition?
        self.assertEqual(repr(tests[0]), "<TestAtJoinNode: parent=<BetaJoinNode:2>, wme.subject == token[1].wme.object>") # c1<->c2 link
        self.assertEqual(repr(tests[1]), "<TestAtJoinNode: parent=<BetaJoinNode:3>, wme.subject == token[2].wme.object>") # c2<->c3 link
        c3tests = list(betajoin[2].tests.all())
        self.assertEqual(len(c3tests), 1)
        # Page 24.
        self.assertEqual(c3tests[0].field_of_arg1, S_IDX)
        self.assertEqual(c3tests[0].condition_number_of_arg2, S_IDX)
        self.assertEqual(c3tests[0].field_of_arg2, O_IDX)
        
        # Confirm betamemorynode tokens.
        tokens = list(models.Token.objects.all())
        self.assertEqual(len(tokens), 4) # dummy, plus one per beta memory node match
        self.assertEqual(tokens[0].parent, None)
        self.assertEqual(tokens[0].index, 0)
        self.assertEqual(tokens[0].wme, None)
        self.assertEqual(list(tokens[0].betamemories.all()), [])
        self.assertEqual(tokens[1].parent, tokens[0])
        self.assertEqual(tokens[1].index, 1)
        self.assertEqual(tokens[1].wme, f1)
        self.assertEqual(list(tokens[1].betamemories.all()), [betamemory[0]])
        self.assertEqual(tokens[2].parent, tokens[1])
        self.assertEqual(tokens[2].index, 2)
        self.assertEqual(tokens[2].wme, f2)#?
        self.assertEqual(list(tokens[2].betamemories.all()), [betamemory[1]])
        
        # Confirm production was activated.
        pnodes = list(models.PNode.objects.all().order_by('id'))
        self.assertEqual(len(pnodes), 1)
        self.assertTrue(pnodes[0].parent)
        pnodes = list(models.PNode.objects.filter(_triggered=True).order_by('id'))
        self.assertEqual(len(pnodes), 1)
        
        facts = []
        facts.append(T('block6', 'on', 'block7'))
        facts.append(T('block7', 'left-of', 'block9'))
        facts.append(T('block9', 'color', 'red'))
        f4,f5,f6 = facts
        for fact in facts:
            rete.add_wme(fact)
        
        tokens = list(models.Token.objects.all())
        self.assertEqual(len(tokens), 7) # dummy, plus one per beta memory node match
        
        matches = [sorted(m, key=lambda t:t.id) for m in pnodes[0].matches]
        self.assertEqual(len(matches), 2)
        self.assertEqual(matches[0], [f1,f2,f3])
        self.assertEqual(matches[1], [f4,f5,f6])
        
        facts = []
        facts.append(T('block89', 'color', 'red'))
        facts.append(T('block77', 'left-of', 'block89'))
        facts.append(T('block66', 'on', 'block77'))
        f4,f5,f6 = facts
        for fact in facts:
            rete.add_wme(fact)
        self.assertEqual(len(list(pnodes[0].matches)), 3)
        
        facts = []
        facts.append(T('block889', 'color', 'red'))
        facts.append(T('block277', 'right-of', 'block889'))
        facts.append(T('block166', 'on', 'block277'))
        f4,f5,f6 = facts
        for fact in facts:
            rete.add_wme(fact)
        self.assertEqual(len(list(pnodes[0].matches)), 3)
        
        # Add a completely different production with no overlap
        # after WME have been added.
        p2 = models.Production.get('rule2', [
            ['?','?x','below','?y'],
            ['?','?y','right-of','?z'],
            ['?','?z','color','blue'],
        ])
        rete.add_production(p2)
        facts = []
        facts.append(T('block77', 'right-of', 'block819'))
        facts.append(T('block819', 'color', 'blue'))
        facts.append(T('block66', 'below', 'block77'))
        f4,f5,f6 = facts
        for fact in facts:
            rete.add_wme(fact)
            
        pnodes = list(models.PNode.objects.all().order_by('id'))
        self.assertEqual(len(list(pnodes)), 2)
        self.assertEqual(len(list(pnodes[0].tokens.all())), 3)
        self.assertEqual(len(list(pnodes[1].tokens.all())), 1)
        self.assertEqual(len(list(pnodes[0].matches)), 3)
        self.assertEqual(len(list(pnodes[1].matches)), 1)
        
        # Add a production that is a subset of a prior production
        # and confirm it automatically has matches.
        p3 = models.Production.get('rule3', [
            ['?','?x','on','?y'],
            ['?','?y','left-of','?z'],
        ])
        rete.add_production(p3)
        self.assertEqual(len(list(p3.pnode.matches)), 3)
        
    def _test_rete_performance(self):
        """
        Measures the rate at which triples and rules are indexed in a RETE
        network.
        """
        import time, random
        
        import django
        from django.conf import settings
        
        from pylab import xlabel, ylabel, title, grid, show, plot
        
        from triple.utils import nested_to_triples
        
        def make_nested(allow_vars=True):
            pattern = {}
            for _ in xrange(random.randint(1,MAX_PARTS)):
                top = random.choice(subjects)
                pattern.setdefault(top, {})
                pred = random.choice(predicates)
                pattern[top].setdefault(pred, {})
                if allow_vars:
                    pattern[top][pred][random.choice(object_predicates)] = random.choice(object_objects) if random.random() > 0.5 else '?'+chr(random.randint(97,122))
                else:
                    pattern[top][pred][random.choice(object_predicates)] = random.choice(object_objects)
                return pattern
        
        rete = models.Rete().save()
        
        x = [10,20,30,40,50,60,70,80,90,100,200,300,400,500]#,600,700,800,900,1000,]#2000,3000,4000,5000,6000,7000,8000,9000,10000]
        y = []
        
        MAX_WME = 50
        MAX_PARTS = 5
        rules = []
        wmes = []
        subjects = [DONT_CARE,'sys','sys1','sys2','me','you','him']
        predicates = ['did','does','has','is','was','just','saw','ran','analyzed','communicated']
        object_predicates = ['func','log','exe','prog','arg0','arg1','arg2','arg3','arg4','arg5','arg6','result']
        object_objects = ['is_mounted','is_dir','is_file','has_mounted','was_mounted','/dev/sda','/dev/sdb','/dev/sdc','/dev/sdd',1,0]
        # Generate random WME.
        while len(wmes) < MAX_WME:
            pattern = make_nested(allow_vars=0)
            triples = nested_to_triples(pattern, as_vars=0)
            wmes.extend([T(*t) for t in triples])
        
        try:
            
            tmp_debug = settings.DEBUG
            settings.DEBUG = False
            django.db.transaction.enter_transaction_management()
            django.db.transaction.managed(True)
            
            for N in x:#,1000,2000,3000]
                    
                # Wipe out all WMEs.
                rete.remove_all_wme()
            
                # Generate N random rule patterns.
                print 'Generating %i rules...' % N
                while len(rules) < N:
                
                    # Build production.
                    pattern = make_nested(allow_vars=1)
                    conditions = nested_to_triples(pattern, as_vars=1)
                    triples = nested_to_triples(pattern, as_vars=0)
                    wmes.extend([T(*t) for t in triples])
                    p = models.Production.get('rule%i'%len(rules), conditions)
                    rules.append(p)
                    rete.add_production(p)
                    
                print 'Committing...'
                django.db.transaction.commit()
                
                # Re-add all WMEs.
    #            print 'Evaluating rule matching performance...'
                t0 = time.time()
                for i,wme in enumerate(wmes):
                    if not i or not i % 50: print '\t',i
                    rete.add_wme(wme)
                t1 = time.time() - t0
                y.append(t1)
                #triggered = rete.pnodes.filter(triggered__gt=0).count()
    #            print 'triggered:',triggered
    #            print 'time:',t1
        except KeyboardInterrupt:
            pass
        finally:
            print 'Committing...'
            settings.DEBUG = tmp_debug
            django.db.transaction.commit()
            django.db.transaction.leave_transaction_management()
            
        plot(x, y, linewidth=2.0)
        xlabel('number of rules')
        ylabel('time (seconds)')
        title('triple rule query scalability')
        grid(True)
        show()
    
    def test_id_match(self):
        from triple.utils import nested_to_triples
        
        rete = models.Rete().save()
        
        # Define production.
        p1 = models.Production.get('p1', [
#            [u'?id1', u'?b795a407-2512-4d3c-a7bf-c46c06e1772a', u'0', u'/dev/sda'],
#            ['?', u'?19b99244-8469-4305-a186-f0a89fc27f7d', u'src', u'?id1'],
            {
                '#sys/sensors/raid/get_all_drive_mountpoints':{
                    'out':{
                        'type':'list',
                        'value':{
                            '0:id=?id1':'/dev/sda',
                            '1':'/dev/sdb',
                        }
                    }
                }
            },
            {
                '#sys/sensors/raid/in_raid':{
                    'in':{
                        'type':'str',
                        'value':'/dev/sda',
                        'src':'?id1'#DONT_CARE
                    },
                    'out':{
                        'type':'bool',
                        'value':'False'
                    }
                }
            }
        ])
        self.assertEqual(models.AlphaNode.objects.all().count(), 0)
        rete.add_production(p1)
        #return
        
        # Define WMEs.
        initial_state = models.State(
        {
            '#sys/sensors/raid/get_all_drive_mountpoints':{
                'out':{
                    'type':'list',
                    'value':{
                        '0:id=?id1':'/dev/sda',
                        '1':'/dev/sdb',
                    }
                }
            }
        },{
            '#sys/sensors/raid/in_raid':{
                'in':{
                    'type':'str',
                    'value':'/dev/sda',
                    'src':'?id1'#DONT_CARE
                },
                'out':{
                    'type':'bool',
                    'value':'False'
                }
            }
        }
        )
        for t in initial_state.iter_triples():
            rete.add_wme(t)
#        triples = []
#        triples.append(T(*[u'someid1', u'0', u'/dev/sda']))
#        triples.append(T(*[u'someid2', u'src', triples[-1].id]))
#        t1,t2 = triples
#        for t in triples:
#            rete.add_wme(t)
        
        # Confirm alphanode memory.
#        anodes = list(models.AlphaNode.objects.all().order_by('id'))
#        self.assertEqual(len(anodes), 4)
#        self.assertEqual(list(anodes[0].items.all()), [t1,t2])
#        self.assertEqual(set(anodes[0].children.all()), set([anodes[1],anodes[3]]))
#        self.assertEqual(list(anodes[1].items.all()), [t1])
#        self.assertEqual(set(anodes[1].children.all()), set([anodes[2]]))
#        self.assertEqual(list(anodes[2].items.all()), [t1])
#        self.assertEqual(list(anodes[3].items.all()), [t2])
#        
#        # Confirm betajoinnode memory.
#        bjoinnodes = list(models.BetaJoinNode.objects.all().order_by('id'))
#        self.assertEqual(len(bjoinnodes), 2)
#        bmem1_tokens = list(bjoinnodes[0].child.tokens.all())
#        self.assertEqual(len(bmem1_tokens), 1)
#        #print bmem1_tokens
#        self.assertEqual([t.wme for t in bmem1_tokens], [t1])
#        self.assertEqual(len(list(bjoinnodes[1].pnodes.all())), 1)
        #self.assertEqual(len(list(bjoinnodes[1].pnodes.all()[0].tokens.all())), 1)
#        print '-'*80
#        nodes = models.AlphaNode.objects.filter(parent=None)
#        for n in nodes:
#            walk_nodes(n)
            
        self.assertEqual(len(list(rete.triggered_pnodes)), 1)

    def test_id_match2(self):
        from triple.utils import nested_to_triples
        
        rete = models.Rete().save()
        
        # Define production.
        p1 = models.Production.get('p1', [
            ['?', u'?19b99244-8469-4305-a186-f0a89fc27f7d', u'src', u'?id1'],
            [u'?id1', u'?b795a407-2512-4d3c-a7bf-c46c06e1772a', u'0', u'/dev/sda'],
        ])
        self.assertEqual(models.AlphaNode.objects.all().count(), 0)
        rete.add_production(p1)
        #return
        
        # Define WMEs.
        triples = []
        triples.append(T(*[u'someid1', u'0', u'/dev/sda']))
        triples.append(T(*[u'someid2', u'src', triples[-1].id]))
        t1,t2 = triples
        for t in triples:
            rete.add_wme(t)
        
        # Confirm alphanode memory.
#        anodes = list(models.AlphaNode.objects.all().order_by('id'))
#        self.assertEqual(len(anodes), 4)
#        self.assertEqual(list(anodes[0].items.all()), [t1,t2])
#        self.assertEqual(set(anodes[0].children.all()), set([anodes[1],anodes[3]]))
#        self.assertEqual(list(anodes[1].items.all()), [t1])
#        self.assertEqual(set(anodes[1].children.all()), set([anodes[2]]))
#        self.assertEqual(list(anodes[2].items.all()), [t1])
#        self.assertEqual(list(anodes[3].items.all()), [t2])
#        
#        # Confirm betajoinnode memory.
#        bjoinnodes = list(models.BetaJoinNode.objects.all().order_by('id'))
#        self.assertEqual(len(bjoinnodes), 2)
#        bmem1_tokens = list(bjoinnodes[0].child.tokens.all())
#        self.assertEqual(len(bmem1_tokens), 1)
#        #print bmem1_tokens
#        self.assertEqual([t.wme for t in bmem1_tokens], [t1])
#        self.assertEqual(len(list(bjoinnodes[1].pnodes.all())), 1)
        #self.assertEqual(len(list(bjoinnodes[1].pnodes.all()[0].tokens.all())), 1)
#        print '-'*80
#        nodes = models.AlphaNode.objects.filter(parent=None)
#        for n in nodes:
#            walk_nodes(n)
            
        self.assertEqual(len(list(rete.triggered_pnodes)), 1)
#        for match in rete.triggered_pnodes[0].matches:
#            for t in match:
#                print t
    
    def test_match(self):
        
        rete = models.Rete().save()
        
        p1 = models.Production.get('p1', [
            {
                '#sys/domains/raid/get_all_drive_mountpoints:id=?id1':{
                    'out':{
                        'type':'list',
                        'value':{
                            '?index':'?mount_point',
                        },
                    }
                }
            },
            {
                '#sys/domains/raid/in_raid':{
                    'in':{
                        'type':'str',
                        'value':'?mountpoint',
                        'src':'?id1'
                    },
                    'out':{
                        'type':'bool',
                        'value':'True',
                    }
                }
            },
            {
                '#sys/domains/raid/has_partitions':{
                    'in':{
                        'type':'str',
                        'value':'?mountpoint',
                        'src':'?id1'
                    },
                    'out':{
                        'type':'bool',
                        'value':'True',
                    }
                }
            }
        ])
        
        initial_state = models.State({
            '#sys/domains/raid/get_all_drive_mountpoints:id=?id1':{
                'out':{
                    'type':'list',
                    'value':{
                        '0':'/dev/sda',
                        '1':'/dev/sdb',
                    }
                }
            }
        },
        {
            '#sys/domains/raid/in_raid':{
                'in':{
                    'type':'str',
                    'value':'/dev/sda',
                    'src':'?id1'#DONT_CARE
                },
                'out':{
                    'type':'bool',
                    'value':'True'
                }
            }
        },
        {
            '#sys/domains/raid/has_partitions':{
                'in':{
                    'type':'str',
                    'value':'/dev/sda',
                    'src':'?id1'
                },
                'out':{
                    'type':'bool',
                    'value':'True',
                }
            }
        }
        )

        rete.add_production(p1)
        
        for wme in initial_state.iter_triples():
            rete.add_wme(wme)
            
        self.assertEqual(len(list(rete.triggered_pnodes.all())), 1)
        
    def test_add_production_post_wme(self):
        
        rete = models.Rete().save()
        
        p1 = models.Production.get('p1', [
            {
                '#sys/domains/raid/get_all_drive_mountpoints:id=?id1':{
                    'out':{
                        'type':'list',
                        'value':{
                            '?index':'?mount_point',
                        },
                    }
                }
            },
            {
                '#sys/domains/raid/in_raid':{
                    'in':{
                        'type':'str',
                        'value':'?mountpoint',
                        'src':'?id1'
                    },
                    'out':{
                        'type':'bool',
                        'value':'True',
                    }
                }
            },
            {
                '#sys/domains/raid/has_partitions':{
                    'in':{
                        'type':'str',
                        'value':'?mountpoint',
                        'src':'?id1'
                    },
                    'out':{
                        'type':'bool',
                        'value':'True',
                    }
                }
            }
        ])
        
        p2 = models.Production.get('p2', [
            {
                '#sys/domains/raid/get_all_drive_mountpoints:id=?id1':{
                    'out':{
                        'type':'list',
                        'value':{
                            '?index':'?mount_point',
                        },
                    }
                }
            },
            {
                '#sys/domains/raid/in_raid':{
                    'in':{
                        'type':'str',
                        'value':'?mountpoint',
                        'src':'?id1'
                    },
                    'out':{
                        'type':'bool',
                        'value':'True',
                    }
                }
            },
        ])
        
        p3 = models.Production.get('p3', [
            {
                '#sys/domains/raid/get_all_drive_mountpoints:id=?id1':{
                    'out':{
                        'type':'list',
                        'value':{
                            '?index':'?mount_point',
                        },
                    }
                }
            },
        ])
        
        p4 = models.Production.get('p4', [
            {
                'laksjdflsjflkdjfls':{
                    'out':{
                        'type':'lskdjflksf',
                        'value':{
                            '?index':'?mount_point',
                        },
                    }
                }
            },
        ])
        
        initial_state = models.State({
            '#sys/domains/raid/get_all_drive_mountpoints:id=?id1':{
                'out':{
                    'type':'list',
                    'value':{
                        '0':'/dev/sda',
                        '1':'/dev/sdb',
                    }
                }
            }
        },
        {
            '#sys/domains/raid/in_raid':{
                'in':{
                    'type':'str',
                    'value':'/dev/sda',
                    'src':'?id1'#DONT_CARE
                },
                'out':{
                    'type':'bool',
                    'value':'True'
                }
            }
        },
        {
            '#sys/domains/raid/has_partitions':{
                'in':{
                    'type':'str',
                    'value':'/dev/sda',
                    'src':'?id1'
                },
                'out':{
                    'type':'bool',
                    'value':'True',
                }
            }
        }
        )
        
        for wme in initial_state.iter_triples():
            rete.add_wme(wme)

        rete.add_production(p1)
        rete.add_production(p2)
        rete.add_production(p3)
        rete.add_production(p4)
            
        self.assertEqual(len(list(rete.triggered_pnodes.all())), 3)

        # Confirm production removal updates triggered pnode set.
        rete.remove_production(p4)
        self.assertEqual(len(list(rete.triggered_pnodes.all())), 3)
        rete.remove_production(p3)
        self.assertEqual(len(list(rete.triggered_pnodes.all())), 2)
        rete.remove_production(p2)
        self.assertEqual(len(list(rete.triggered_pnodes.all())), 1)
        rete.remove_production(p1)
        self.assertEqual(len(list(rete.triggered_pnodes.all())), 0)
        
    def test_variable_extraction(self):
        
        rete = models.Rete().save()
        
        p1 = models.Production.get('p1', [
            {
                '#sys/domains/raid/get_all_drive_mountpoints:id=?id1':{
                    'out':{
                        'type':'list',
                        'value':{
                            '?index':'?mount_point',
                        },
                    }
                }
            },
        ])
            
        initial_state = models.State({
            '#sys/domains/raid/get_all_drive_mountpoints:id=?id1':{
                'out':{
                    'type':'list',
                    'value':{
                        '0':'/dev/sda',
                        '1':'/dev/sdb',
                    }
                }
            }
        },
        {
            '#sys/domains/raid/in_raid':{
                'in':{
                    'type':'str',
                    'value':'/dev/sda',
                    'src':'?id1'#DONT_CARE
                },
                'out':{
                    'type':'bool',
                    'value':'True'
                }
            }
        },
        {
            '#sys/domains/raid/has_partitions':{
                'in':{
                    'type':'str',
                    'value':'/dev/sda',
                    'src':'?id1'
                },
                'out':{
                    'type':'bool',
                    'value':'True',
                }
            }
        }
        )
        
        for wme in initial_state.iter_triples():
            rete.add_wme(wme)

        rete.add_production(p1)
        
        triggered_pnodes = list(rete.triggered_pnodes.all())
        self.assertEqual(len(triggered_pnodes), 1)
        pnode = triggered_pnodes[0]
        matches = list(pnode.matches)
        self.assertEqual(len(matches), 2)
        self.assertEqual(len(matches[0]), 4)
        self.assertEqual(len(matches[1]), 4)
        match_vars = frozenset([frozenset([(k,v) for k,v in mv.iteritems() if k in ['mount_point','index','id1']]) for mv in pnode.match_variables])
        self.assertEqual(match_vars, frozenset([frozenset([(u'index', u'1'), (u'mount_point', u'/dev/sdb'), (u'id1', 1)]),
                                                frozenset([(u'mount_point', u'/dev/sda'), (u'index', u'0'), (u'id1', 1)])]))

    def test_pnode_trigger_stack(self):
        
        rete = models.Rete().save()
        
        p1 = models.Production.get('p1',[
            ['?','?x','on','?y'],
        ])
        rete.add_production(p1)
        
        p2 = models.Production.get('p2',[
            ['?','?y','left-of','?z'],
        ])
        rete.add_production(p2)
        
        p3 = models.Production.get('p3',[
            ['?','?z','color','red'],
        ])
        rete.add_production(p3)
        
        self.assertEqual(len(list(rete.triggered_pnodes)), 0)
        self.assertEqual(rete.top_pnode_trigger_stack, None)
        
        rete.add_wme(T('block1', 'on', 'block2'))
        self.assertEqual(len(list(rete.triggered_pnodes)), 1)
        self.assertEqual(rete.top_pnode_trigger_stack, None)
        
        rete.push_pnode_trigger_stack()
        rete.add_wme(T('block2', 'left-of', 'block3'))
        self.assertEqual(len(list(rete.triggered_pnodes)), 1)
        self.assertNotEqual(rete.top_pnode_trigger_stack, None)
        self.assertEqual(len(rete.top_pnode_trigger_stack.pnodes.all()), 1)
        
        rete.add_wme(T('block3', 'color', 'red'))
        self.assertEqual(len(list(rete.triggered_pnodes)), 2)
        self.assertEqual(len(rete.top_pnode_trigger_stack.pnodes.all()), 2)
        
        rete.push_pnode_trigger_stack()
        rete.add_wme(T('block11', 'on', 'block21'))
        self.assertEqual(len(list(rete.triggered_pnodes)), 1)
        self.assertEqual(len(rete.top_pnode_trigger_stack.pnodes.all()), 1)
        self.assertEqual([len(pnode_group.pnodes.all()) for pnode_group in rete.pnode_trigger_stack.all().order_by('-id')], [1,2])
        
        rete.add_wme(T('block21', 'left-of', 'block31'))
        rete.add_wme(T('block31', 'color', 'red'))
        self.assertEqual([len(pnode_group.pnodes.all()) for pnode_group in rete.pnode_trigger_stack.all().order_by('-id')], [3,2])
        
        rete.pop_pnode_trigger_stack()
        self.assertEqual([len(pnode_group.pnodes.all()) for pnode_group in rete.pnode_trigger_stack.all().order_by('-id')], [2])
        
        rete.pop_pnode_trigger_stack()
        self.assertEqual([len(pnode_group.pnodes.all()) for pnode_group in rete.pnode_trigger_stack.all().order_by('-id')], [])

    def test_inequality(self):
        import time
        from triple.constants import CURRENT, DELETED, SUBJECT
        from triple.models import GID
        
        rete = models.Rete().save()
        
        p1 = models.Production.get('p1',[
            ['?id1','?x','on','?y'],
            ['?','?id1','created','?created1'],
            ['?id2','?y','left-of','?z'],
            ['?','?id2','created','?created2'],
            ['float(?created1) < float(?created2)'],
            ['?','?z','above','?z2'],
        ])
        rete.add_production(p1)
        
        betajoins = list(models.BetaJoinNode.objects.all().order_by('id'))
        self.assertEqual(len(betajoins), 6)
        
        tests = list(betajoins[0].tests.all())
        self.assertEqual(len(tests), 0)
        
        tests = list(betajoins[1].tests.all())
        self.assertEqual(len(tests), 1)
        self.assertEqual(repr(tests[0]), "<TestAtJoinNode: parent=<BetaJoinNode:2>, wme.subject == token[1].wme.id>")
        
        tests = list(betajoins[2].tests.all())
        self.assertEqual(len(tests), 1)
        self.assertEqual(repr(tests[0]), "<TestAtJoinNode: parent=<BetaJoinNode:3>, wme.subject == token[1].wme.object>")
        
        tests = list(betajoins[3].tests.all())
        self.assertEqual(len(tests), 1)
        self.assertEqual(repr(tests[0]), "<TestAtJoinNode: parent=<BetaJoinNode:4>, wme.subject == token[3].wme.id>")
        
        tests = list(betajoins[4].tests.all())
        self.assertEqual(len(tests), 1)
        self.assertEqual(repr(tests[0]), "<TestAtJoinNode: parent=<BetaJoinNode:5>, float(_v(2,4)) < float(_v(4,4))>")
        
        tests = list(betajoins[5].tests.all())
        self.assertEqual(len(tests), 1)
        self.assertEqual(repr(tests[0]), "<TestAtJoinNode: parent=<BetaJoinNode:6>, wme.subject == token[3].wme.object>")
        
        t1 = T('block1', 'on', 'block2', gid=[CURRENT])
        t2 = T(t1.id, 'created', time.time(), gid=[CURRENT])
        rete.add_wme(t1)
        rete.add_wme(t2)
        self.assertEqual(len(list(rete.triggered_pnodes)), 0)
        
        t3 = T('block2', 'left-of', 'block3', gid=[CURRENT])
        t4 = T(t3.id, 'created', time.time(), gid=[CURRENT])
        rete.add_wme(t3)
        
        alphanodes = list(models.AlphaNode.objects.all().order_by('id'))
        self.assertEqual(len(alphanodes), 5)
        self.assertEqual(list(alphanodes[0].items.all()), [t1,t2,t3])
        self.assertEqual(list(alphanodes[1].items.all()), [t1])
        self.assertEqual(list(alphanodes[2].items.all()), [t2])
        self.assertEqual(list(alphanodes[3].items.all()), [t3])
        self.assertEqual(list(alphanodes[4].items.all()), [])
        
        self.assertEqual(list(alphanodes[0]._successors.all().order_by('id')), [])
        self.assertEqual(list(alphanodes[1]._successors.all().order_by('id')), [betajoins[0]])
        self.assertEqual(list(alphanodes[2]._successors.all().order_by('id')), [betajoins[1],betajoins[3],betajoins[4]])
        self.assertEqual(list(alphanodes[3]._successors.all().order_by('id')), [betajoins[2]])
        self.assertEqual(list(alphanodes[4]._successors.all().order_by('id')), [betajoins[5]])
        
        self.assertNotEqual(betajoins[0].alphanode, None)
        self.assertEqual(betajoins[0].alphanode, alphanodes[1])
        self.assertEqual(betajoins[0].parent, None)
        self.assertEqual(betajoins[1].alphanode, None)
        self.assertEqual(betajoins[1]._alphanode, alphanodes[2])
        self.assertEqual(betajoins[1].parent.parent, betajoins[0])
        self.assertEqual(betajoins[2].alphanode, None)
        self.assertEqual(betajoins[2]._alphanode, alphanodes[3])
        self.assertEqual(betajoins[2].parent.parent, betajoins[1])
        self.assertEqual(betajoins[3].alphanode, None)
        self.assertEqual(betajoins[3]._alphanode, alphanodes[2])
        self.assertEqual(betajoins[3].parent.parent, betajoins[2])
        self.assertEqual(betajoins[4].alphanode, None)
        self.assertEqual(betajoins[4]._alphanode, alphanodes[2])
        self.assertEqual(betajoins[4].parent.parent, betajoins[3])
        self.assertEqual(betajoins[5].alphanode, None)
        self.assertEqual(betajoins[5]._alphanode, alphanodes[4])
        self.assertEqual(betajoins[5].parent.parent, betajoins[4])
        
        tokens = list(models.Token.objects.all())
        self.assertEqual(len(tokens), 4)
        
        tokens = list(betajoins[0].child.tokens.all())
        wmes = [(t.id,t.index,t.parent.id,t.wme) for t in tokens]
        self.assertEqual(wmes, [(2,1,1,t1)])
        
        tokens = list(betajoins[1].child.tokens.all())
        wmes = [(t.id,t.index,t.parent.id,t.wme) for t in tokens]
        self.assertEqual(wmes, [(3,2,2,t2)])
        
        tokens = list(betajoins[2].child.tokens.all())
        wmes = [(t.id,t.index,t.parent.id,t.wme) for t in tokens]
        self.assertEqual(wmes, [(4,3,3,t3)])
        
        tokens = list(betajoins[3].child.tokens.all())
        wmes = [(t.id,t.index,t.parent.id,t.wme) for t in tokens]
        self.assertEqual(wmes, [])
        
        rete.add_wme(t4)
        
        tokens = list(betajoins[3].child.tokens.all())
        wmes = [(t.id,t.index,t.parent.id,t.wme) for t in tokens]
        self.assertEqual(wmes, [(5,4,4,t4)])
        
        #self.assertEqual(len(list(rete.triggered_pnodes)), 1)
        rete.add_wme(T('block3', 'above', 'block4'))
        
        self.assertEqual(len(list(rete.triggered_pnodes)), 1)
        
    def _test_ReteTripleImportQueue(self):
        """
        Test queuing triples for entry into a RETE network
        and iteratively processing the queue.
        """
        #TODO:fix? nondeterministically fails
        import time
        from triple.constants import CURRENT, DELETED, SUBJECT
        from triple.models import GID
        
        rete0 = models.Rete().save()
        rete1 = models.Rete().save()
        
        p1 = models.Production.get('p1',[
            ['?id1','?x','on','?y'],
            ['?','?id1','created','?created1'],
            ['?id2','?y','left-of','?z'],
            ['?','?id2','created','?created2'],
            ['float(?created1) < float(?created2)'],
            ['?','?z','above','?z2'],
        ])
        rete1.add_production(p1)
        
        t1 = T('block1', 'on', 'block2', gid=[CURRENT])
        t2 = T(t1.id, 'created', time.time(), gid=[CURRENT])
        t3 = T('block2', 'left-of', 'block3', gid=[CURRENT])
        t4 = T(t3.id, 'created', time.time(), gid=[CURRENT])
        t5 = T('block3', 'above', 'block4', gid=[CURRENT])
        rete1.add_wme(t1)
        rete1.add_wme(t2)
        rete1.add_wme(t3)
        rete1.add_wme(t4)
        rete0.add_wme(t5)
        rete1.add_wme(t5)
        
        self.assertEqual(len(list(rete0.triggered_pnodes)), 0)
        self.assertEqual(len(list(rete1.triggered_pnodes)), 1)
        
        t5new = models.update_wme(t5, SUBJECT, 'blockx')
        self.assertNotEqual(t5, t5new)
        self.assertEqual(list(t5.graphs.all()), [GID(DELETED)])
        self.assertEqual(list(t5new.graphs.all()), [GID(CURRENT)])
        
        # And confirm that Rete shows incorrect triggers, because it hasn't
        # been notified of the update.
        self.assertEqual(len(list(rete0.triggered_pnodes)), 0)
        self.assertEqual(len(list(rete1.triggered_pnodes)), 1)
        
        ## Confirm queues.
        
        queues = list(models.ReteTripleImportQueue.objects.all())
        self.assertEqual(len(queues), 4)
        
        queues = list(models.ReteTripleImportQueue.objects.filter(triple=t5))
        self.assertEqual(len(queues), 2)
        self.assertEqual(queues[0].rete, rete0)
        self.assertEqual(queues[0]._delete, True)
        self.assertEqual(queues[1].rete, rete1)
        self.assertEqual(queues[1]._delete, True)
        
        queues = list(models.ReteTripleImportQueue.objects.filter(triple=t5new))
        self.assertEqual(len(queues), 2)
        self.assertEqual(queues[0].rete, rete0)
        self.assertEqual(queues[0]._delete, False)
        self.assertEqual(queues[1].rete, rete1)
        self.assertEqual(queues[1]._delete, False)
        
        ## Run rete cycles and confirm queues are processed.
        
        runs = list(rete0.iter_run())
        self.assertEqual(len(runs), 0)
        self.assertEqual(len(list(rete0.triggered_pnodes)), 0)
        
        self.assertEqual(models.ReteTripleImportQueue.objects.all().count(), 1)
        
        runs = list(rete1.iter_run())
        self.assertEqual(len(runs), 0)
        self.assertEqual(len(list(rete1.triggered_pnodes)), 0)
        
        self.assertEqual(models.ReteTripleImportQueue.objects.all().count(), 0)
        