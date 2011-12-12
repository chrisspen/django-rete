# -*- coding: utf-8 -*-

import decimal, re, time, datetime, cPickle as pickle, base64, uuid

from django.contrib.contenttypes import generic
from django.contrib.contenttypes.models import ContentType
from django.db import models
from django.db.models.query_utils import Q
from django.utils.encoding import smart_str, smart_unicode

from triple.constants import \
    ID_IDX, \
    S_IDX, \
    P_IDX, \
    O_IDX, \
    FIELDS, \
    FIELD_INDEXES, \
    MAX_LENGTH, \
    FIELD_IDX_TO_NAME, \
    DONT_CARE

from triple.models import Triple
from triple.utils import nested_to_triples

from constants import *

def _print(*args):
#    print ' '.join(map(str, args))
    return

def is_variable(value):
    """
    Returns true if the given string represents a variable, denoted by
    starting with a "?".
    """
    return isinstance(value, basestring) and value.strip().startswith('?')

class _BaseModel(models.Model):
    
    class Meta:
        abstract = True
        
    def save(self, *args, **kwargs):
        super(_BaseModel, self).save(*args, **kwargs)
        return self

class Condition(_BaseModel):
    """
    Helper class for manipulating production conditions.
    Represents a pattern meant to match one or more triples.
    """
    #TODO:relocate production link to Production, so a condition (linked to a transition) can be used in multiple productions?
    #production = models.ForeignKey('Production', blank=True, null=True, related_name='conditions')
    _id = models.CharField(max_length=MAX_LENGTH, blank=True, null=True, db_index=True)
    _subject = models.CharField(max_length=MAX_LENGTH, blank=True, null=True, db_index=True)
    _predicate = models.CharField(max_length=MAX_LENGTH, blank=True, null=True, db_index=True)
    _object = models.CharField(max_length=MAX_LENGTH, blank=True, null=True, db_index=True)
    _expression = models.CharField(max_length=MAX_LENGTH, blank=True, null=True, db_index=True)
    
    # Implied fields:
    #    self.productions = [Production]
    
    class Meta:
        unique_together = [
            ('_id','_subject','_predicate','_object','_expression'),
        ]
    
    def __str__(self):
        return repr(self)
    
    def __repr__(self):
        if self._expression:
            return "<%s:%s>" % (type(self).__name__, self._expression)
        else:
            return "<%s:%s>" % (type(self).__name__, str(self.parts))
    
    @property
    def subject(self):
        return self._subject
    
    @property
    def predicate(self):
        return self._predicate
    
    @property
    def object(self):
        return self._object
    
    @property
    def expression(self):
        return self._expression
    
    @property
    def test_variables(self):
        return set(re.findall("\?[a-zA-Z0-9_]+", self._expression))
    
    @classmethod
    def get(cls, production, *parts):
        """
        Helper function for building conditions.
        """
        import ast
        assert production is None or isinstance(production, Production)
        assert (len(parts) == 1 and isinstance(parts[0],basestring)) or len(parts) == len(FIELD_INDEXES), "Invalid parts length: %s" % (str(parts),)
        if len(parts) == 1:
            expression = parts[0]
            # Escape variables.
            _vars = re.findall("\?[a-zA-Z0-9_]+", expression)
            _expression = expression
            for _var in _vars:
                _expression = _expression.replace(_var, _var[1:])
            # Confirm test contains a valid Python expression.
            ret = ast.parse(_expression)
            assert len(ret.body) == 1 and type(ret.body[0]).__name__ == 'Expr', "Invalid test. Must contain a valid Python expression."
            c,_ = cls.objects.get_or_create(_id=None,
                                            _subject=None,
                                            _predicate=None,
                                            _object=None,
                                            _expression=expression)
        else:
            id,sub,pred,obj = parts
            #assert id == DONT_CARE or id is None or isinstance(id, int) or (isinstance(id,basestring) and id.isdigit()), "Inline tests not supported for condition ID field. This value must be an integer. Parts: %s" % (str(parts),)
            c,_ = cls.objects.get_or_create(_id=None if id == DONT_CARE or id is None else id,
                                            _subject=None if sub == DONT_CARE or sub is None else sub,
                                            _predicate=None if pred == DONT_CARE or pred is None else pred,
                                            _object=None if obj == DONT_CARE or obj is None else obj)
        if production:
            production.conditions.add(c)
        return c
    
    @property
    def parts(self):
        """
        Returns a list of condition attributes ordered according to
        FIELD_INDEXES.
        """
        return [self._id if self._id else DONT_CARE,
                self._subject if self._subject else DONT_CARE,
                self._predicate if self._predicate else DONT_CARE,
                self._object if self._object else DONT_CARE]
    
    @property
    def constant_tests(self):
        """
        Returns an iterator of the form:
        
            [(field index, operator index, field value)]
        
        representing constant triple tests.
        """
        for field_idx,field_value in zip(FIELD_INDEXES,self.parts):
            #print field_idx,field_value
            
            # Skip fields whose value is not a constant.
            if is_variable(field_value):
                continue
                
            # Extract operator from field value.
            # If missing, assumed to be equality.
            op_idx = EQ_IDX
            if not isinstance(field_value, unicode):
                field_value = unicode(field_value,'utf-8')
            #print field_value[0]
            if field_value[0] in OPERATION_NAMES:
                op_idx = OP_NAME_TO_IDX[field_value[0]]
                field_value = field_value[1:]
            
            yield field_idx,op_idx,field_value
    
    @property
    def variable_bindings(self):
        """
        Returns an iterator of the form:
        
            [(field index, variable name)]
        
        representing variable names bound to certain fields.
        """
        for field_idx,field_value in zip(FIELD_INDEXES,self.parts):
            
            # Skip fields whose value is not a constant.
            if not (is_variable(field_value) and len(field_value) >= 2):
                continue
        
            yield field_idx, field_value[1:]

class ConditionGroup(_BaseModel):
    """
    Helper model to partition and organize conditions.
    """
    
    # A set of conditions belonging to the group.
    conditions = models.ManyToManyField(Condition)
    
    # The branch to which this group belongs.
    branch = models.PositiveIntegerField(default=0, blank=False, null=False, db_index=True)
    
    def get_interpolated_conditions(self, match_vars):
        """
        Returns its conditions with variables replaced according to the given
        {var_name:var_value} map.
        """
        assert isinstance(match_vars, dict)
        return [Condition.get(None, *[match_vars.get(part[1:],part) if is_variable(part) else part for part in c.parts]) for c in self.conditions.all()]

class ReteTripleImportQueue(_BaseModel):
    """
    Represents a first-in-first-out queue of triples that need to be entered or
    updated in Rete network.
    """
    rete = models.ForeignKey('Rete', blank=False, null=False, related_name='triple_import_queue')
    triple = models.ForeignKey(Triple, blank=False, null=False)
    created_datetime = models.DateTimeField(auto_now_add=True, blank=False, null=False, db_index=True)
    _delete = models.BooleanField(default=False, blank=False, null=False)
    
    class Meta:
        ordering = ['rete','created_datetime','triple',]
        
    @classmethod
    def push(cls, triple, rete=None, delete=False):
        """
        Pushes a triple onto the queue for the given Rete network.
        """
        # Find Retes that use the triple.
        anodes = AlphaNode.objects.filter(parent__isnull=True, items__id=triple.id)
        retes = set([anode.rete for anode in anodes]).difference([rete])
        for _rete in retes:
            cls(rete=_rete, triple=triple, _delete=delete).save()
        if rete:
            cls(rete=rete, triple=triple, _delete=delete).save()
    
    @classmethod
    def pop(cls, rete):
        """
        Removes a tuple of the format (triple,delete).
        'Triple' is the earliest triple in the queue for the given Rete
        network. 'Delete' is a boolean flag indicating if the triple
        should be deleted from the given Rete network.
        Returns None if the queue is empty.
        """
        q = cls.objects.filter(rete=rete)
        if q.count():
            record = q[0]
            triple = record.triple
            delete = record._delete
            record.delete()
            return triple,delete

def update_wme(t, field, value):
    """
    Updates a triple safely so that all Rete networks using it remain
    consistent.
    """
    from triple.models import Triple, GID
    from triple.constants import FIELD_NAMES, CURRENT, DELETED
    assert isinstance(t, Triple)
    assert field in FIELD_NAMES, "Field must be one of %s." % (str(FIELD_NAMES),)
    
    # Create new updated triple.
    tnew = t.copy()
    setattr(tnew, field, value)
    tnew.save()
    t.graphs.remove(GID(CURRENT))
    t.graphs.add(GID(DELETED))
    
    # Queue the old triple for removal from all Rete networks.
    ReteTripleImportQueue.push(t, delete=True)
    
    # Add the new triple to all the Rete networks using the old triple.
    anodes = AlphaNode.objects.filter(parent__isnull=True, items__id=t.id)
    retes = set([anode.rete for anode in anodes])
    for rete in retes:
        ReteTripleImportQueue.push(triple=tnew, rete=rete, delete=False)
        
    return tnew

class Rete(_BaseModel):
    """
    Encapsulates the top-level interface for a RETE network
    as outlined in "Production Matching for Large Learning Systems"
    by Robert Doorenbos.
    """
    name = models.CharField(max_length=100, blank=True, null=True, unique=True, db_index=True)
    
    pnodes = models.ManyToManyField('PNode', related_name='retes')
    
    pnode_trigger_stack = models.ManyToManyField('PNodeGroup', related_name="rete_trigger_stacks")
    
    # Implied fields:
    #    alphanodes := [AlphaNode]
    
    @property
    def alphanode_top(self):
        """
        Returns the alphanode instance at the top of the alphanode network.
        """
        node,_ = AlphaNode.objects.get_or_create(rete=self,
                                                 field=None,
                                                 operation=None,
                                                 value=None,
                                                 parent=None)
        return node
    
    @property
    def items(self):
        return self.alphanode_top.items.all()
    
    @property
    def top_pnode_trigger_stack(self):
        """
        Returns the most recent PNodeGroup added to the trigger stack.
        """
        groups = self.pnode_trigger_stack.all().order_by('-id')
        if groups.count():
            return groups[0]
    
    @property
    def triggered_pnodes(self):
        return self.pnodes.exclude(_triggered=0)
    
    def add_production(self, production):
        """
        Populates the RETE network from the production's conditions.
        [Production Matching for Large Learning Systems, Page 37]
        """
        assert isinstance(production, Production)
        assert production.conditions.all().count() >= 1, "Production must have at least 1 condition."
        #TODO:check for production name already existing in RETE?
        
        current_node = None
        bc = {} # binding_condition, {variable name: index of last condition to reference this variable}
        bf = {} # binding_field, {variable name: index of last field to reference this variable}
        all_tests = []
        earlier_conds = []
        
        conditions = list(production.conditions.all().order_by('id'))
        condition = conditions[0]
        _print('creating tests for',condition)
        tests = self.get_join_tests_from_condition(condition, earlier_conds, bc, bf)
        _print('created tests:',tests,'for',condition)
        alphanode = self.build_or_share_alpha_memory(condition)
        current_node0 = current_node
        current_node = self.build_or_share_join_node(current_node, alphanode, tests)
        _print('created',current_node,'for',current_node0,'with',alphanode)
        condition0 = condition
        last_betajoinnode = current_node
        
        for condition in conditions[1:]:
#            t0 = time.time()
            current_node = self.build_or_share_beta_memory_node(current_node)
#            t1 = time.time()-t0
            #print 'build_or_share_beta_memory_node:secs:\t%.3f'%t1
            
            if condition0:
                earlier_conds.append(condition0)
                
#            t0 = time.time()
            tests = self.get_join_tests_from_condition(condition, earlier_conds, bc, bf)
#            t1 = time.time()-t0
            #print 'get_join_tests_from_condition:secs:\t%.3f'%t1
            
            _print('created tests:',tests,'for',condition)
            
            if condition.expression:
                #assert isinstance(last_betajoinnode, BetaJoinNode)
                # Append additional tests onto the last beta join node.
                #current_node = last_betajoinnode
#                for test in tests:
#                    test.parent = current_node
#                    test.save()
                
                current_node0 = current_node
                current_node = self.build_or_share_join_node(current_node, alphanode, tests)
                # Don't include this condition in index calculations.
                condition0 = None
            else:
    #            t0 = time.time()
                alphanode = self.build_or_share_alpha_memory(condition)
    #            t1 = time.time()-t0
    #            print 'build_or_share_alpha_memory:secs:\t%.3f'%t1
                
                current_node0 = current_node
                current_node = self.build_or_share_join_node(current_node, alphanode, tests)
                
                condition0 = condition
                _print('created',current_node,'for',current_node0,'with',alphanode)
                
                last_betajoinnode = current_node
            
        # Create and link pnode.
        pnode = PNode(production=production, parent=current_node).save()
        self.pnodes.add(pnode)
#        t0 = time.time()
        self.update_new_node_with_matches_from_above(pnode)
#        t1 = time.time() - t0
#        print 'update_new_node_with_matches_from_above secs:',t1
        return pnode
    
    def add_wme(self, triple, force_recheck=False):#, anode_id_list=None):
        """
        Adds working memory element to the RETE network.
        [Production Matching for Large Learning Systems, Page 14-15]
        """
        assert isinstance(triple, Triple)
#        if not force_recheck and self.items.filter(id=triple.id).count():
#            return False
        _print('ADDING WME:',triple)
        self.constant_test_node_activation(self.alphanode_top, triple, force_recheck=force_recheck)

    def build_or_share_alpha_memory(self, condition):
        """
        Adds a production condition to the network by
        creating alpha/memory nodes.
        
        This is a helper function for creating a new alpha memory for a given
        condition, or finding an existing one to share. The implementation of
        this function depends on what type of alpha net implementation is used.
        If we use a traditional data ow network, as described in Section 2.2.1,
        then we simply start at the top of the alpha network and work our way
        down, sharing or building new constant test nodes:
        
        [Production Matching for Large Learning Systems, Page 35].
        
        Returns the last alphanode/memory used by the condition.
        """
        #TODO:?
        assert isinstance(condition, Condition)
        current_node = self.alphanode_top
        _print('created',current_node)
        
#        t0 = time.time()
        travelled_anodes = [current_node]
        created_anodes = []
        for field_idx,op_idx,field_value in condition.constant_tests:
            current_node,created = self.build_or_share_constant_test_node(current_node, field_idx, op_idx, field_value)
            _print('created',current_node,'for',field_idx,op_idx,field_value,'in condition',condition)
            travelled_anodes.append(current_node)
            if created:
                created_anodes.append(current_node)
#        t1 = time.time()-t0
#        print 'build_or_share_alpha_memory.build_tests:secs:\t%.3f'%t1
        
        # Initialize tail alphamemory with any current WMEs.
        # Only iterate over WMEs contained in superset of new anodes,
        # by iterating over all WMEs (triples) contained in the parent
        # alphanodes of alphanodes that were just created (i.e. were not
        # pre-existing).
        # See page 36-37 for details. Is this correct?
#        t0 = time.time()
        #wmes = self.items.all() # Note, very slow!
        wmes = reduce(lambda a,b:a.union(b), [anode.parent.items.all() for anode in created_anodes], set())
        for wme in wmes:
            self.add_wme(wme, force_recheck=True)
#        t1 = time.time()-t0
#        print 'build_or_share_alpha_memory.add_wme:secs:\t%.3f'%t1
        
        return current_node
    
    def build_or_share_beta_memory_node(self, parent):
        """
        Creates a BetaMemoryNode.
        [Production Matching for Large Learning Systems, Page 34]
        """
        assert isinstance(parent, BetaJoinNode)
        if parent.child:
            return parent.child
        new_node = BetaMemoryNode().save()
        parent.child = new_node
        parent.save()
        assert new_node.parent == parent
        self.update_new_node_with_matches_from_above(new_node)
        return new_node

    def build_or_share_constant_test_node(self, parent, field_idx, op_idx, field_value):
        """
        Creates or reuses an alphanode to match the given constant test.
        
        [Production Matching for Large Learning Systems, Page 36].
        """
        node,_ = AlphaNode.objects.get_or_create(rete=self,
                                                 field=field_idx,
                                                 operation=op_idx,
                                                 value=field_value,
                                                 parent=parent)
        return node,_
    
    def build_or_share_join_node(self, betamemory, alphanode, tests):
        """
        Creates a BetaJoinNode.
        [Production Matching for Large Learning Systems, Page 34]
        """
        assert betamemory is None or isinstance(betamemory, BetaMemoryNode), "Betamemory must be of type BetaMemoryNode, not %s." % (type(betamemory).__name__,)
        assert isinstance(alphanode, AlphaNode), "Alphanode must be of type AlphaNode, not %s." % (type(alphanode).__name__,)
        if betamemory:
            for child in betamemory._children.all():
                if isinstance(child, BetaJoinNode) and child.alphanode == alphanode and child.tests == tests:
                    return child
            
        bn = BetaJoinNode()
        bn.alphanode = alphanode
        bn._alphanode = alphanode
        if betamemory:
            #betamemory.child = bn
            #betamemory.save()
            bn.betamemory = betamemory
            bn._betamemory = betamemory
        bn.save()
            
        bn.check_right_linking()
        bn.check_left_linking()
        
        for test in tests:
            test.parent = bn
            test.save()
        return bn
    
    def constant_test_node_activation(self, node, wme, force_recheck=False, level=0):
        """
        Tests the working memory element for a match against the current alphanode.
        Similar to the exhaustive hash table method outlined in:
        [Production Matching for Large Learning Systems, Page 14-17]
        """
        assert isinstance(node, AlphaNode)
        assert isinstance(wme, Triple)
            
        # Record the match at the current alphanode.
        node.memory_activation(wme, force_recheck=force_recheck)
        
        # Find child alphanodes with a matching pattern for all fields.
        for field_idx in FIELD_INDEXES:
            
            # Lookup triple field value.
            field_value = getattr(wme, FIELD_IDX_TO_NAME[field_idx])
#            _print(' '*(level*4),FIELD_IDX_TO_NAME[field_idx],field_value)
            
            # Find alphanodes whose condition matches the given field value.
            q = Q(operation=EQ_IDX, value=field_value)
#            q |= Q(Q(operation=NE_IDX), ~Q(value=field_value))
#            try:
#                # If value is numeric, then append numeric tests.
#                numeric_value = decimal.Decimal(field_value)
#                q |= Q(operation=LT_IDX, value__gt=numeric_value)
#                q |= Q(operation=GT_IDX, value__lt=numeric_value)
#                q |= Q(operation=LE_IDX, value__gte=numeric_value)
#                q |= Q(operation=GE_IDX, value__lte=numeric_value)
#                q |= Q(Q(operation=NE_IDX), ~Q(value=numeric_value))
#            except decimal.InvalidOperation:
#                # Otherwise, only do equality test.
#                pass
            children = self.alphanodes.filter(Q(parent=node),
                                           Q(field=field_idx),
                                           q
                                        )
            
            # Propagate match through the alphanode network.
#            all_children = self.alphanodes.filter(parent=node)
#            _print(' '*(level*4),'constant_test_node_activation.children:',all_children.count())
#            for c in all_children:
#                _print(' '*((level+1)*4),c)
#            _print(' '*(level*4),'constant_test_node_activation.children:',children.count())
            for child in children:
                self.constant_test_node_activation(child, wme, force_recheck=force_recheck, level=level+1)
    
    def delete_alpha_memory(self, alphanode):
        """
        Described, but not explicitly given.
        [Production Matching for Large Learning Systems, Page 39]
        """
        TODO
        
    def delete_node_and_any_unused_ancestors(self, node):
        """
        [Production Matching for Large Learning Systems, Page 39]
        """
        if isinstance(node, BetaJoinNode):
            alphanode = node.alphanode
            node.alphanode = None
            node.save()
            if not alphanode.successors.all().count():
                self.delete_alpha_memory(alphanode)
        else:
            assert type(node) in (PNode, BetaMemoryNode), "Invalid node type: %s" % (type(node).__name__,)
            for token in node.tokens.all():
                self.delete_token_and_descendents(token)
        #remove node from the list node.parent.children
        parent = node.parent
        parent.child = None
        parent.save()
        if not list(parent.children):
            self.delete_node_and_any_unused_ancestors(parent)
        #deallocate memory for node
        node.delete()
    
    def delete_token_and_descendents(self, token, level=0):
        """
        Removes a token and all descendent tokens.
        [Production Matching for Large Learning Systems, Page 31]
        
        Update to support right-linking/unlinking.
        [Production Matching for Large Learning Systems, Page 87]
        """
        _print(' '*(level*4),'deleting token:',token)
        _print(' '*(level*4),'token children:',token.children.all().count())
        for child in list(token.children.all()):
            _print(' '*(level*4),'deleting token %s from child %s' % (token, child))
            self.delete_token_and_descendents(child, level=level+1)
            
        #remove tok from the list tok.node.items
        for bm in list(token.betamemories.all()):
            _print(' '*(level*4),'removing token %s from betamemory %s' % (token, bm))
            bm.tokens.remove(token)
            if not bm.tokens.all().count():
                # If betamemory node mepty, then unlink betajoinnodes from
                # their alphanodes.
                for bm_child in bm._children.all():
                    bm_child.unlink_right()
            
        #remove tok from pnodes
        _print(' '*(level*4),'token pnodes:',token.pnodes.all().count())
        for pnode in list(token.pnodes.all()):
            _print(' '*(level*4),'removing token %s from pnode %s' % (token, pnode))
            pnode.remove_token(token)
            pnode._triggered -= 1
            pnode.save()
        
        #remove tok from the list tok.parent.children
        token.parent.children.remove(token)#TODO:?
        
        #remove tok from the list tok.wme.tokens
        token.delete()
    
    def get_join_tests_from_condition(self, condition, earlier_conds, binding_condition, binding_field):
        """
        Creates join tests from conditions. These describe the relationships
        between variables contained across multiple conditions.
        [Production Matching for Large Learning Systems, Page 35]
        """
        assert isinstance(condition, Condition)
        assert isinstance(binding_condition, dict)
        assert isinstance(binding_field, dict)
        tests = []
        _binding_condition = {} # {variable name:last condition index using variable name}
        _binding_field = {} # {variable name:last field index used}
        #for i,condition in enumerate(conditions):
        if condition.expression:
            test = TestAtJoinNode()
            # Convert variables within the expression into:
            # (field,condition-index) format.
            vars = condition.test_variables
            expr = condition.expression
            for var in vars:
                var_name = var[1:]
                #assert binding_field[var_name] in FIELD_INDEXES
                expr = expr.replace(var, "_v(%i,%i)" % (binding_condition[var_name],binding_field[var_name]))
            test.expression = expr
            test.save()
            tests.append(test)
        else:
            for field_idx,var_name in condition.variable_bindings:
    #            print '\t',field_idx,var_name,len(earlier_conds)+1
                
                # If variable name occurs anywhere in earlier-conds then create a test.
                if var_name in binding_condition:
                    test = TestAtJoinNode()
                    test.field_of_arg1 = field_idx
                    test.condition_number_of_arg2 = binding_condition[var_name]#TODO:correct?
                    test.field_of_arg2 = binding_field[var_name]
                    test.save()
                    tests.append(test)
                
                # Record usage for next iteration.
                _binding_condition[var_name] = len(earlier_conds)+1 # 1-indexed of last test, TODO:fix? index of condition, or test?
                _binding_field[var_name] = field_idx
            
        binding_condition.update(_binding_condition)
        binding_field.update(_binding_field)
        return tests
        
    def pop_pnode_trigger_stack(self):
        """
        Removes the top PNodeGroup from the trigger stack.
        Any PNodes that activate thereafter will be added to the next top
        group, if any exist.
        """
        groups = self.pnode_trigger_stack.all().order_by('-id')
        group = None
        if groups.count():
            group = groups[0]
            self.pnode_trigger_stack.remove(group)
        return group
    
    def push_pnode_trigger_stack(self):
        """
        Adds a new PNodeGroup to the top of the trigger stack.
        Any PNodes that activate thereafter will be added to this group.
        """
        next_index = self.pnode_trigger_stack.all().count()
        group = PNodeGroup(index=next_index).save()
        self.pnode_trigger_stack.add(group)
        for pnode in self.pnodes.all():
            pnode._triggered = 0
            pnode.save()
        return group
        
    def remove_production(self, production):
        """
        Removes a production and all it's dependencies from the network.
        [Production Matching for Large Learning Systems, Page 38]
        """
        pnode = production.pnode
        self.delete_node_and_any_unused_ancestors(pnode)
        self.pnodes.remove(pnode)
    
    def remove_all_wme(self):
        """
        Removes all working memory elements from the network.
        """
        for anode in self.alphanodes.all():
            for bjoin in anode.successors.all():
                # Clear beta memory nodes.
                if bjoin.child:
                    tokens = list(bjoin.child.tokens.all())
                    bjoin.child.tokens.clear()
                    for token in tokens:
                        token.delete()
                # Clear pnodes.
                for pnode in bjoin.pnodes.all():
                    #TODO:Delete all tokens at all pnode groups?
                    tokens = list(pnode.tokens.all())
                    pnode.tokens.clear()
                    for token in tokens:
                        token.delete()
            anode.items.clear()
        
    def remove_wme(self, wme):
        """
        Removes a working memory element from the RETE network.
        [Production Matching for Large Learning Systems, Page 30]
        
        Updated to support left-unlinking.
        [Production Matching for Large Learning Systems, Page 102]
        """
        assert isinstance(wme, Triple)
        if not self.items.filter(id=wme.id).count():
            _print('skipping removal')
            return
        
        # Remove WME from alphanode memories.
        for an in list(wme.alphanodes.all()):
            an.items.remove(wme)
            #TODO:Fix left-unlinking.
#            if not an.items.count():
                #TODO:Don't left-unlink negative nodes.
#                for betanode in an._successors.all():
#                    betanode.unlink_left()
            
        # Delete tokens.
        tokens = Token.objects.filter(wme=wme)#.exclude(id__in=Token.objects.filter(parent__isnull=False).values_list('parent'))
        _print('deleting tokens:',tokens.count())
#        for token in tokens:
#            print token,token.parent
        #return
        for token in tokens:
            self.delete_token_and_descendents(token)
    
    def update_new_node_with_matches_from_above(self, new_node):
        """
        Updates new beta memory or beta join nodes with match data.
        [Production Matching for Large Learning Systems, Page 38]
        """
        parent = new_node.parent
        if isinstance(parent, BetaMemoryNode):
            for token in parent.tokens.all():
                new_node.left_activation(token)
        elif isinstance(parent, BetaJoinNode):
            if parent.alphanode:
                for wme in parent.alphanode.items.all():
                    parent.right_activation(wme, children=[new_node])
        else:
            raise Exception, "Unknown parent type '%s' for %s node." % (type(parent).__name__, type(new_node).__name,)

    def iter_run(self):
        """
        Iterates over match-eval cycles until no more productions are
        triggered.
        """
        from triple.constants import CURRENT
        while 1:
            
            # Process pending triple additions or updates.
            while 1:
                top_anode = self.alphanode_top
                result = ReteTripleImportQueue.pop(rete=self)
                if not result:
                    break
                triple,delete = result
                print 'handling triple queue:',triple,delete
                
                # Remove the triple from the network if it's already
                # been added.
                if top_anode.items.filter(id=triple.id).count():
                    print '\tremoving:',triple
                    self.remove_wme(triple)
                    
                if delete:
                    # Check triple for usage, and delete if not used.
                    _anodes = AlphaNode.objects.filter(parent__isnull=True,
                                                       items__id=triple.id)
                    if not _anodes.count():
                        print '\tpermanently deleting:',triple
                        triple.delete()
                else:
                    # Add the triple to the network if we're not deleting it.
                    print '\tadding:',triple
                    self.add_wme(triple)
            
            pnodes = self.triggered_pnodes
            if not pnodes.count():
                # Stop if no rules have triggered.
                return
            
            # Process each triggered pnode.
            pending_updates = []
            pending_adds = []
            for pnode in pnodes:
                for vars in pnode.match_variables:
                    for effect in pnode.production.effects:
#                        print effect
                        if isinstance(effect, Update):
                            pending_updates.append((effect,vars))
                        else:
                            pending_adds.extend(effect._do(self, vars, graphs=[CURRENT]))
                        
            yield pending_updates, pending_adds
            print 'pending_updates:',len(pending_updates)
            print 'pending_adds:',len(pending_adds)
            
            # Reset trigger counts.
            self.push_pnode_trigger_stack()
            
            # Apply pending updates.
            for update_effect,update_vars in pending_updates:
                print '\tupdating:',update_effect,update_vars
                pending_adds.extend(update_effect._do(self, update_vars))
                
            # Apply pending adds.
            for wme in pending_adds:
                print '\tadding:',wme
                self.add_wme(wme)

class AlphaNode(_BaseModel):
    """
    Tests a working memory element for a constant condition.
    """
    
    # The specific rete network to which we belong.
    rete = models.ForeignKey(Rete, blank=False, null=False, related_name='alphanodes')
    
    # Field in the WME to test.
    field = models.IntegerField(choices=FIELDS, blank=True, null=True, db_index=True)
    
    # Field operation to use when testing.
    operation = models.IntegerField(default=EQ_IDX, choices=OPERATIONS, blank=True, null=True, db_index=True)
    
    # Field value in the WME to test.
    value = models.CharField(max_length=MAX_LENGTH, blank=True, null=True, db_index=True)
    
    # WME (triples) that have matched the condition.
    items = models.ManyToManyField(Triple, related_name='alphanodes')
    
    # Links to our parent alpha node in the dataflow network.
    parent = models.ForeignKey('self', blank=True, null=True, related_name='children')
    
    # Implied fields:
    #     self.successors = set(linked BetaJoinNodes)
    #     self._successors = set(BetaJoinNodes)
    
    class Meta:
        unique_together = [('rete','parent','field','operation','value')]
        ordering = ['rete','parent__id','field','operation','value']
    
    def __str__(self):
        return repr(self)
    
    def __repr__(self):
        field_name = self.field_name
        operation_name = self.operation_name
        value = self.value
        return smart_str(u"<%s:%i %s %s %s>" % (type(self).__name__, self.id, field_name, operation_name, value))
    
    @property
    def field_name(self):
        return FIELD_IDX_TO_NAME.get(self.field)
    
    @property
    def operation_name(self):
        return OP_IDX_TO_NAME.get(self.operation)
    
    def memory_activation(self, wme, force_recheck=False):
        """
        Adds a working memory element to the alphanode's memory.
        [Production Matching for Large Learning Systems, Page 21]
        
        Updated to support left-unlinking.
        [Production Matching for Large Learning Systems, Page 102]
        """
        #_print('act.mem:',self,wme,self.successors.all().count(),self._successors.all().count())
        assert isinstance(wme, Triple)
        
        # Only right_activate if wme is newly added?
#        if not force_recheck and self.items.filter(id=wme.id).count():
#            return
            
        self.items.add(wme)
#        if not self.successors.all().count():
#            _print('    no successors!')

        # Ensure our non-right-linked beta join nodes are left-linked.
        # Note, right-linked beta join nodes will be checked after their
        # right activation.
        #TODO:Fix left-unlinking.
#        for betanode in self._successors.exclude(id__in=self.successors.all()):
#            betanode.link_left()
#        for betanode in self._successors.all():
#            betanode.link_left()

        # Right activate right-linked beta join nodes.
        for betanode in self.successors.all().order_by('-id'):
            betanode.right_activation(wme=wme, level=1)
    
class BetaMemoryNode(_BaseModel):
    """
    Stores partial instantiations of productions (i.e. partial matches).
    """
    
    # The BetaJoinNode this memory feeds into on the "left" side.
    #child = models.OneToOneField('BetaJoinNode', blank=True, null=True, related_name='_parent')
    
    # A list of WME lists representing partial matches.
    tokens = models.ManyToManyField('Token', related_name="betamemories")

    # Implicit fields:
    #    self.parent => BetaJoinNode
    #    self.children => [BetaJoinNode]
    #    self._children => [BetaJoinNode]

    def __str__(self):
        return repr(self)

    def __repr__(self):
        token_str = ' ' + (", ".join(map(repr, self.tokens.all())))
        if not token_str.strip():
            token_str = ''
        return "<%s:%i%s>" % (type(self).__name__, self.id, token_str)

#    @property
#    def children(self):
#        if self.child:
#            yield self.child

    def left_activation(self, token, wme, level=0):
        """
        Builds a token, adds it to the beta memory's list of tokens, and
        informs each child.
        [Production Matching for Large Learning Systems, Page 23]
        """
        _print(' '*level,'act.left:',self,token,wme)
        assert isinstance(token, Token)
        assert isinstance(wme, Triple)
        new_token,created = Token.objects.get_or_create(parent=token, index=token.index+1, wme=wme)
        #if not self.tokens.filter(id=new_token.id).count():#TODO:is this correct?
        self.tokens.add(new_token)
        for child in self.children.all():
            child.left_activation(new_token, level=level+1)

class TestAtJoinNode(_BaseModel):
    """
    "...specifies the locations of the two felds whose values must be equal in
    order for some variable to be bound consistently."
    [Production Matching for Large Learning Systems, Page 24]
    """
    parent = models.ForeignKey('BetaJoinNode', blank=True, null=True, related_name="tests")
    
    # Field in the WME1 in the alpha memory to test.
    field_of_arg1 = models.IntegerField(choices=FIELDS, blank=True, null=True, db_index=True)
    
    # The index of the condition of field 2 in the production's LHS.
    condition_number_of_arg2 = models.IntegerField(blank=True, null=True, db_index=True)
    
    # Field in the WME2, matched from some earlier condition, to test.
    field_of_arg2 = models.IntegerField(choices=FIELDS, blank=True, null=True, db_index=True)
    
    # Logical Python expression.
    expression = models.CharField(max_length=MAX_LENGTH, blank=True, null=True, db_index=True)
    
    def __repr__(self):
        if self.expression:
            args = (
                type(self).__name__,
                "<%s:%s>" % (type(self.parent).__name__, self.parent.id if self.parent else None,),
                self.expression,
            )
            return "<%s: parent=%s, %s>" % args
        else:
            args = (
                type(self).__name__,
                "<%s:%s>" % (type(self.parent).__name__, self.parent.id if self.parent else None,),
                FIELD_IDX_TO_NAME[self.field_of_arg1],
                self.condition_number_of_arg2,
                FIELD_IDX_TO_NAME[self.field_of_arg2],
            )
            return "<%s: parent=%s, wme.%s == token[%i].wme.%s>" % args

    def __str__(self):
        return repr(self)

class BetaJoinNode(_BaseModel):
    """
    Tests for consistency of variable bindings between conditions.
    Joins a beta memory node (on the "left") and an alpha memory node
    (on the "right").
    """
    
    # Right.
    alphanode = models.ForeignKey(AlphaNode, blank=True, null=True, related_name='successors')
    _alphanode = models.ForeignKey(AlphaNode, blank=False, null=False, related_name='_successors')
    
    # Left.
    betamemory = models.ForeignKey(BetaMemoryNode, blank=True, null=True, related_name='children')
    _betamemory = models.ForeignKey(BetaMemoryNode, blank=True, null=True, related_name='_children')
    
    child = models.OneToOneField('BetaMemoryNode', blank=True, null=True, related_name='parent')
    
    #Implicit fields:
    #    self.pnodes.all() => set of PNode objects, a type of children
    #    self.parent => a BetaMemoryNode object
    #    self.tests.all() => set of TestAtJoinNode objects
     
    class Meta:
        # To ensure descendent nodes (with a higher id) come before ancester nodes (with a lower id), per page 27?
        ordering = ['-id']
    
    @property
    def is_linked_right(self):
        return self.alphanode is not None
    
    def link_right(self):
        self.alphanode = self._alphanode
        self.save()
        
    def unlink_right(self):
        self.alphanode = None
        self.save()
    
    def check_right_linking(self):
        """
        Sets the correct right-linking for the current node,
        based on the contents of its parent betamemory.
        """
        if self.parent:
            if not self.is_linked_right and self.parent.tokens.all().count():
                self.link_right()
            elif self.is_linked_right and not self.parent.tokens.all().count():
                self.unlink_right()
        elif not self.is_linked_right:
            self.link_right()
    
    @property
    def is_linked_left(self):
        return self.betamemory is not None
    
    def link_left(self):
        self.betamemory = self._betamemory
        self.save()
        
    def unlink_left(self):
        return#TODO:Fix left-unlinking.
        self.betamemory = None
        self.save()
    
    def check_left_linking(self):
        """
        Sets the correct left-linking for the current node,
        based on the contents of its parent alphanode.
        """
        return#TODO:Fix left-unlinking.
        if not self.is_linked_left and self._alphanode.items.all().count():
            self.link_left()
        elif self.is_linked_left and not self._alphanode.items.all().count():
            self.unlink_left()
    
    @property
    def parent(self):
        try:
            return self._betamemory
        except BetaMemoryNode.DoesNotExist:
            pass
    
    @property
    def children(self):
        if self.child:
            yield self.child
        for pnode in self.pnodes.all():
            yield pnode
    
    def __str__(self):
        return repr(self)
    
    def __repr__(self):
        tests_str = ', '.join(map(repr, self.tests.all()))
        #return "<%s:%i parent=%s, alphanode=%s, tests=[%s]>" % (type(self).__name__, self.id, repr(parent), repr(self.alphanode), tests_str,)
        return "<%s:%i>" % (type(self).__name__, self.id,)
    
    def left_activation(self, token, level=0):
        """
        Upon a left activation (when a new token t is added to the beta
        memory), we look through the alpha memory and find any WME(s) w for
        which all these t-versus-w tests succeed. Again, any successful <t,w>
        combinations are passed on to the node's children.

        [Production Matching for Large Learning Systems, Page 24-25]
        
        Updated to support right-linking/unlinking.
        [Production Matching for Large Learning Systems, Page 88]
        """
        _print(' '*level,'act.left:',self,token)
        assert isinstance(token, Token)
#        if not self.alphanode.items.all().count():
#            _print(' '*level,'no alphanode.items!')

        # Re-link ourselves if our parent betamemory is non-empty.
        self.check_right_linking()

        for wme in self.alphanode.items.all():
            _print(' '*level,'testing token:',token,wme)
            if self.perform_join_tests(token, wme, level=level):
                _print(' '*level,'success!')
                for child in self.children:
                    child.left_activation(token, wme, level=level+1)
            else:
                _print(' '*level,'failure!')
    
    def right_activation(self, wme, children=None, level=0):
        """
        Upon a right activation (when a new WME w is added to the alpha
        memory), we look through the beta memory and nd any token(s) t for
        which all these t-versus-w tests succeed.
        
        Any successful <t,w> combinations are passed on to the join node's
        children.

        [Production Matching for Large Learning Systems, Page 24-25]
        
        Note use of dummy top token...
        [Production Matching for Large Learning Systems, Page 20]
        
        Updated to support left-unlinking.
        [Production Matching for Large Learning Systems, Page 103]
        
        Parameters:
        
        children := Optional list of children, to restrict left activation.
        """
        _print(' '*level,'act.right:',self)
        assert isinstance(wme, Triple)
        
        # Re-link ourselves if our parent alphanode is non-empty.
        self.check_left_linking()
        
        #TODO:correct? page 25 mentions a dummy betamemory with dummy token should be used, but doesn't specify what data these hold
        if self.parent:
            tokens = self.parent.tokens.all()
        else:
            #TODO:is this how the dummy top token should be handled?
            dummy_token,_ = Token.objects.get_or_create(parent=None, wme=None)
            tokens = [dummy_token]
        for token in tokens:
            _print(' '*level,'testing token:',token,wme)
            if self.perform_join_tests(token, wme, level=level):
                _print(' '*level,'success!')
                for child in (children or self.children):
                    child.left_activation(token, wme, level=level+1)
            else:
                _print(' '*level,'failure!')
    
    def perform_join_tests(self, token, wme, level=0):
        """
        [Production Matching for Large Learning Systems, Page 25]
        """
        #TODO:extend this to support inequality functions
        #e.g. this would be necessary for ?created_timestamp1 < ?created_timestamp2
        assert isinstance(token, Token)
        assert isinstance(wme, Triple)
        
        def _v(condition_index, field_index):
            wme = token.get_wme(condition_index)
            value = getattr(wme, FIELD_IDX_TO_NAME[field_index])
            _print(' '*level,'_v(%i,%i) == %s' % (condition_index, field_index, str(value)))
            return value
        
        ret = True
        for test in self.tests.all():
            _print(' '*level,'perform_join_tests:',test)
            if test.expression:
                _print('!'*80)
                _print(' '*level,'BJN:',self.id)
                _print(' '*level,'token:',token.id)
                _print(' '*level,'evaling:',test.expression)
                #continue#TODO:remove
                expr_ret = eval(test.expression, globals(), dict(_v=_v))
                _print(' '*level,'RESULT:',test.expression,'==',expr_ret)
                if not expr_ret:
                    ret = False
                    break
            else:
                arg1 = getattr(wme, FIELD_IDX_TO_NAME[test.field_of_arg1])
                wme2 = token.get_wme(test.condition_number_of_arg2)
                _print(' '*level,'wme1:',wme)
                _print(' '*level,'wme2:',wme2)
                arg2 = getattr(wme2, FIELD_IDX_TO_NAME[test.field_of_arg2])
                
                # Convert triple field to integer if it's being tested against the
                # ID field.
                if test.field_of_arg1 == ID_IDX or test.field_of_arg2 == ID_IDX:
                    if isinstance(arg1, basestring) and arg1.isdigit():
                        arg1 = int(arg1)
                    if isinstance(arg2, basestring) and arg2.isdigit():
                        arg2 = int(arg2)
                        
                if arg1 != arg2:
                    _print(' '*level,'RESULT:',arg1,type(arg1),'!=',arg2,type(arg2))
                    ret = False
                    break
        return ret
    
class Token(_BaseModel):
    """
    Represents a list of working memory elements.
    Contained inside a BetaMemoryNode node.
    """
    #TODO:is this necessary? or can it be replaced with ManyToMany(Triple)?
    parent = models.ForeignKey('self', blank=True, null=True, related_name='children')
    index = models.PositiveIntegerField(default=0, blank=False, null=False, db_index=True)
    wme = models.ForeignKey(Triple, blank=True, null=True)
    
    # Implied fields:
    #    betamemories = [BetaMemory]
    #    pnodes = [PNode]
    #    children = [Token]
    
    def __str__(self):
        return repr(self)
    
    def __repr__(self):
        parent_id = None
        if self.parent:
            parent_id = self.parent.id
        return "<%s: wme=%s, index=%i, parent=%s>" % (type(self).__name__, repr(self.wme), self.index, str(parent_id))
    
    def get_wme(self, index):
        """
        Retrieves the working memory element associated with the given index.
        """
        #TODO:does index==0 correspond to the current wme? or the furthest from the current token?
        place = self
        _print('get_wme:',index,self.index)
        assert isinstance(index, int) and index <= self.index, "Invalid index %s. Must be an integer less than or equal to %i." % (str(index), self.index)
        while place.index != index:
            assert place.parent is not None, "Index out of range: %i" % (index,)
            place = place.parent
        return place.wme
    
    def get_list(self):
        """
        Retrieves the list of working memory elements
        by traversing the token parent links starting
        from the current token.
        """
        wme = []
        place = self
        while place.parent:
            wme.append(place.wme)
#            print place.wme
            place = place.parent
        return wme

class ProductionTag(_BaseModel):
    
    # Unique descriptor.
    name = models.CharField(max_length=50, blank=False, null=False, unique=True, db_index=True)
    
class Production(_BaseModel):
    
    # Unique descriptor.
    name = models.CharField(max_length=50, blank=True, null=True, unique=True, db_index=True)
    
    conditions = models.ManyToManyField(Condition, related_name='productions')
    
    # An arbitrary object representing the production's RHS.
#    _rhs_type = models.ForeignKey(ContentType, blank=True, null=True, db_index=True)
#    _rhs_id = models.PositiveIntegerField(blank=True, null=True, db_index=True)
#    rhs = generic.GenericForeignKey('_rhs_type', '_rhs_id')
    
    _rhs_pickle = models.TextField(db_column='rhs_pickle', null=True, blank=True)
    
    @property
    def effects(self):
        if self._rhs_pickle is None:
            return []
        return pickle.loads(base64.decodestring(self._rhs_pickle))
        
    @effects.setter
    def effects(self, obj):
        self._rhs_pickle = base64.encodestring(pickle.dumps(obj))
    
    tags = models.ManyToManyField(ProductionTag, related_name='productions')
    
    # Implied fields:
    #    conditions = [Condition] # Conditions composing the LHS.
    
    def __str__(self):
        return repr(self)
    
    def __repr__(self):
        return "<%s:%s>" % (type(self).__name__, self.name)
    
    @classmethod
    def get(cls, name, conditions, effects=None):
        """
        Helper function for creating a production.
        """
        p,_ = cls.objects.get_or_create(name=name)
        p.conditions.clear()
        new_conditions = set()
        
        # Convert compact nested-dict notation to expanded conditions.
        _conditions = []
        for condition in conditions:
            if isinstance(condition, dict):
                _conditions.extend(nested_to_triples(condition, as_vars=1))
            else:
                _conditions.append(condition)
        conditions = _conditions
                
        for condition in conditions:
            if type(condition) in (tuple,list):
                condition = Condition.get(p, *condition)
            assert isinstance(condition, Condition), "Unknown condition type: %s" % (condition,)
            new_conditions.add(condition)
            #assert condition.production is None or condition.production == p, "Condition already assigned to production %s" % (p,)
            #condition.production = p
            condition.save()
            p.conditions.add(condition)
        #assert new_conditions == set(p.conditions.all()), "Production contains additional conditions previously defined."
        
        if effects is not None:
            assert isinstance(effects, list)
            p.effects = effects
            p.save()
        
        return p
    
class PNodeGroup(_BaseModel):
    index = models.PositiveIntegerField(default=0, blank=False, null=False)
    pnodes = models.ManyToManyField('PNode', related_name="groups")
    
    class Meta:
        ordering = ['-id']
    
class PNodeTokenGroup(_BaseModel):
    pnodegroup = models.ForeignKey(PNodeGroup)
    pnode = models.ForeignKey('PNode', related_name="pnodetokengroups")
    tokens = models.ManyToManyField('Token', related_name="pnodetokengroups")
    
    # If true, indicates this production had its LHS matched at least once.
    triggered = models.PositiveIntegerField(default=0, blank=False, null=False, db_index=True)
    
class PNode(_BaseModel):
    """
    Links a production to beta join node.
    """
    #rete = models.ForeignKey(Rete, blank=False, null=False, related_name='pnodes')
    
    # The full production object specifying LHS and RHS data.
    production = models.OneToOneField(Production, blank=False, null=False, related_name='pnode')
    
    # The last join node in the network linking to us.
    parent = models.ForeignKey(BetaJoinNode, blank=False, null=False, related_name='pnodes')
    
    # If true, indicates this production had its LHS matched at least once.
    _triggered = models.PositiveIntegerField(default=0, blank=False, null=False, db_index=True)
    
    # A list of WME lists representing full matches.
    tokens = models.ManyToManyField('Token', related_name="pnodes")
    
    # Implied fields:
    #    groups = [PNodeGroup]
    
    @property
    def rete(self):
        return self.retes.all()[0]
    
    @property
    def triggered(self):
        top_group = self.rete.top_pnode_trigger_stack
        if top_group:
            return token_group.triggered
        else:
            return self._triggered
    
    def left_activation(self, token, wme, level=0):
        """
        Similar to BetaMemoryNode's left_activation().
        Records the final token chain and marks the production as triggered.
        """
        _print(' '*level,'act.left:',self,token,wme)
        assert isinstance(token, Token)
        assert isinstance(wme, Triple)
        _print('!'*80)
#        print token, wme
        _print('pnode activated!')
        
        new_token, _ = Token.objects.get_or_create(parent=token, index=token.index+1, wme=wme)
        
        # Record our activation in our rete's trigger stack.
        #for rete in self.retes.all():
        top_group = self.rete.top_pnode_trigger_stack
        if top_group:
            # Update versioned token group.
            top_group.pnodes.add(self)
            token_group,_ = PNodeTokenGroup.objects.get_or_create(pnodegroup=top_group, pnode=self)
            token_group.tokens.add(new_token)
            token_group.triggered += 1
        else:
            # Update master token group.
            self.tokens.add(new_token)
            
        self._triggered += 1
        self.save()
    
    def remove_token(self, token):
        top_group = self.rete.top_pnode_trigger_stack
        if top_group:
            token_group,_ = PNodeTokenGroup.objects.get_or_create(pnodegroup=top_group, pnode=self)
            return token_group.tokens.remove(token)
        else:
            return self.tokens.remove(token)
    
    @property
    def token_list(self):
        top_group = self.rete.top_pnode_trigger_stack
        if top_group:
            token_group,_ = PNodeTokenGroup.objects.get_or_create(pnodegroup=top_group, pnode=self)
            return token_group.tokens.all()
        else:
            return self.tokens.all()
    
    @property
    def matches(self):
        """
        Iterates over each unique match set.
        """
        for token in self.token_list:
            ret = token.get_list()
#            print ret
            yield ret
#            print '---'
    
    @property
    def match_variables(self):
        """
        Iterates over the variable bindings for each match set.
        """
        conditions = self.production.conditions.all().order_by('id')
        for match in self.matches:
            vars = {} # {var_name:var_value}
            triples = reversed(match)
            for c,t in zip(conditions,triples):
                for field_idx,var_name in c.variable_bindings:
                    if var_name in vars:
                        continue
                    field_value = getattr(t, FIELD_IDX_TO_NAME[field_idx])
                    if field_value == DONT_CARE:
                        continue
                    vars[var_name] = field_value
            yield vars
    
    def __repr__(self):
        return "<%s:%i>" % (type(self).__name__, self.id,)
    
    def __str__(self):
        return repr(self)

class State(object):
    """
    List of literal triple patterns
    (i.e. they should contain no variables).
    They should be directly convertable to triples.
    """
    #TODO:Merge with ConditionGroup?
    
    def __init__(self, *parts):
        from triple.utils import nested_to_triples
        self.parts = []
        for part in parts:
            if isinstance(part, dict):
                self.parts.extend(nested_to_triples(part, as_vars=0))
            elif isinstance(part, Condition):
                self.parts.append(part.parts)
            else:
                raise Exception, "Invalid part type: %s" % (type(part).__name__,)
    
    def to_condition_group(self):
        cg = ConditionGroup().save()
        for c in self.iter_conditions(to_vars=True):
            cg.conditions.add(c)
        return cg
    
    def iter_conditions(self, to_vars=False):
        """
        Parameters:
            to_vars := If true, converts all intermediary identifiers, that match
                       the internal UUID, into variables.
        """
        uuid_pattern = re.compile("^(#)[a-zA-Z0-9]{8}\-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{12}$")
        for parts in self.parts:
            if to_vars:
                parts = ['?'+part[1:] if uuid_pattern.findall(part) else part for part in parts]
            yield Condition.get(None, *parts)
            
    def iter_triples(self, allow_unsatisfied_variables=False):
        """
        Creates triples from the given parts and returns
        a generator iterating over them. 
        """
        from triple.models import T
        id_key = {} # {key:id}
        #id_pattern = re.compile(":id=(\?[a-zA-Z0-9_]+)")
        id_pattern = re.compile("\?[a-zA-Z0-9_\-]+")
        for part in self.parts:
            #print part
            id,s,p,o = part
            #print id,s,p,o
            for match in id_pattern.findall(unicode(s)):
                #print 'match:',match
                if match in id_key or not allow_unsatisfied_variables:
                    s = s.replace(match, id_key[match])
            for match in id_pattern.findall(unicode(p)):
                #print 'match:',match
                if match in id_key or not allow_unsatisfied_variables:
                    p = p.replace(match, id_key[match])
            for match in id_pattern.findall(unicode(o)):
                #print 'match:',match
                if match in id_key or not allow_unsatisfied_variables:
                    o = o.replace(match, id_key[match])
            wme = T(*[s,p,o])
            if len(id) > 1:
                id_key[id] = str(wme.id)
            #print 'ADDING:',wme
            yield wme

class _ReteAction(object):

    def __init__(self):
        pass
    
    def _lookup(self, value, vars):
        assert isinstance(vars, dict)
        if isinstance(value, basestring) and value.startswith('?') and len(value) > 1:
            value = vars[value[1:]]
        return value
    
    def _do(self, rete, vars, *args, **kwargs):
        assert isinstance(rete, Rete)
        assert isinstance(vars, dict)
        return self.do(rete, vars, *args, **kwargs)
        
class Create(_ReteAction):
    
    def __init__(self, parts, var_map=None):
        import ast
        self.parts = parts
        for part in parts:
            assert isinstance(part, dict)
        self.var_map = var_map or {}
        for k,v in self.var_map.iteritems():
            
            # Confirm test contains a valid Python expression.
            ret = ast.parse(v)
            assert len(ret.body) == 1 and type(ret.body[0]).__name__ == 'Expr', "Invalid value. Must contain a valid Python expression: %s" % (v,)
        
    def _lookup(self, value, vars):
        from triple.utils import dt
        assert isinstance(vars, dict)
        if isinstance(value, basestring) and value.startswith('?') and len(value) > 1:
            key = value[1:]
            if key in vars:
                # Lookup value from variable map.
                value = super(Create, self)._lookup(value, vars)
            else:
                # Lookup value from function map.
                value = eval(self.var_map[key], globals(), locals())
                vars[key] = value # Save to variable map.
        return value
    
    def do(self, rete, vars, graphs):
        from triple.models import T
        facts = []
        for part in self.parts:
            for fact in nested_to_triples(part, as_vars=0):
                facts.append(T(*[self._lookup(f, vars) for f in fact[1:]], gid=graphs))
#        for f in facts:
#            print f
        return facts
        
class Update(_ReteAction):
    
    def __init__(self, triple_id, field_name, new_field_value):
        self.triple_id = triple_id
        self.field_name = field_name
        self.new_field_value = new_field_value
        
    def do(self, rete, vars):
        id = int(self._lookup(self.triple_id, vars))
        t = Triple.objects.get(id=id)
        rete.remove_wme(t)
        setattr(t, self.field_name, self.new_field_value)
        t.save()
        #rete.add_wme(t)
        return [t]
