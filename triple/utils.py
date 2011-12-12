import uuid

from constants import DONT_CARE

def nested_to_triples(nested, parent_value=None, as_vars=False, id=None):
    """
    Converts the short-hand nested dictionary structure into a series of
    triples.
    
    e.g. Given {'sys2': {'does': {'func': '?z'}}}
    
    With as_vars = False, would produce literals:
    
    ['?', 'sys2', '#e4e8e3cd-7022-464d-ab87-bc5519f06137']
    ['#e4e8e3cd-7022-464d-ab87-bc5519f06137', 'does', '#a41f811b-a1ad-40fb-a4fd-9316425b891a']
    ['#a41f811b-a1ad-40fb-a4fd-9316425b891a', 'func', '?z']
    
    Or with as_vars = True, would produce patterns:
    
    ['?', '?', 'sys2', '?e4e8e3cd-7022-464d-ab87-bc5519f06137']
    ['?', '?e4e8e3cd-7022-464d-ab87-bc5519f06137', 'does', '?a41f811b-a1ad-40fb-a4fd-9316425b891a']
    ['?', '?a41f811b-a1ad-40fb-a4fd-9316425b891a', 'func', '?z']

    If parent_value is given, it's inserted as the subject for the new triples.
    """
    triples = []
    if isinstance(nested, dict):
        nested = nested.items()
    if parent_value is None:
        for k,v in nested:
            triples.extend(nested_to_triples(v, parent_value=k, as_vars=as_vars))
    else:
        for predicate,rest in nested:
            if parent_value is None:
                subject = DONT_CARE
            else:
                subject = parent_value
                
            # Extract inline conditions.
            # e.g. somevalue:id=?id1
            id = DONT_CARE
            extra = None
            if ':' in subject:
                subject,extra = subject.split(':')[0],subject.split(':')[1:]
                for ex in extra:
                    attr,val = ex.split('=')
                    assert attr in ('id',), "Only id inline attribute supported."
                    exec "%s = '%s'" % (attr,val)
            if ':' in predicate:
                predicate,extra = predicate.split(':')[0],predicate.split(':')[1:]
                for ex in extra:
                    attr,val = ex.split('=')
                    assert attr in ('id',), "Only id inline attribute supported."
                    exec "%s = '%s'" % (attr,val)
                
            if type(rest) in (tuple, list, dict):
                # Composite object.
                if as_vars:
                    object = '?'+str(uuid.uuid4())
                else:
                    object = '#'+str(uuid.uuid4())
                if as_vars:
                    triples.append([id,subject,predicate,object])
                else:
                    triples.append([id,subject,predicate,object])
                triples.extend(nested_to_triples(rest, parent_value=object, as_vars=as_vars))
            else:
                # Literal object.
                #assert isinstance(rest, basestring)
                if isinstance(rest, basestring) and ':' in rest:
                    rest,extra = rest.split(':')[0],rest.split(':')[1:]
                    for ex in extra:
                        attr,val = ex.split('=')
                        assert attr in ('id',), "Only id inline attribute supported."
                        exec "%s = '%s'" % (attr,val)
                        
                if as_vars:
                    triples.append([id,subject,predicate,rest])
                else:
                    triples.append([id,subject,predicate,rest])
    return triples

import time
from datetime import datetime, timedelta

#datetime.fromtimestamp(time.mktime(datetime.utcnow().timetuple()))
def dt(*args):
    if args:
        if len(args) == 1 and isinstance(args[0], datetime):
            d = args[0]
        else:
            d = datetime(*args)
    else:
        d = datetime.utcnow()
    #return datetime.fromtimestamp(time.mktime(d.timetuple()))
    return time.mktime(d.timetuple())
