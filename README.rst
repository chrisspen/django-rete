=======================================================================
django-rete - The RETE-UL algorithm implemented on top of Django's ORM.
=======================================================================

Overview
========

Implements a many-to-many database search based on the RETE-UL algorithm
described by Robert Doorenbos in the paper 
"Production Matching for Large Learning Systems".

Allows creation of an arbitrary number of RETE networks, with the addition of
productions and working memory elements (WME) in any SQL database supported by
Django.

Note, this is not an attempt at implementing a complete production rule system.
It's only the RETE component for performing a many-to-many query.

I decided to implement RETE-UL to clarify and test my own understanding of the
algorithm. I chose to use Django as the underlying platform for a few reasons.
First, because I was familiar with Django's ORM. And second, I was curious to
evaluate an out-of-core database-backed RETE algorithm and was not aware of any
public implementations.

This was started as a personal research project, and as such many features were
not implemented or fully tested. Therefore, this project is intended more as a
curiosity and is in no way appropriate for production use or as a replacement
for traditional mature and robust in-memory RETE implementations (e.g. Clips).

A simple Django-based triple store (actually a quad store) is also included,
as a means of storing working memory elements (WME) to feed into RETE networks.

Features
--------

* WME addition and removal
* production addition and removal
* retrieval of WME sets that match each production
* right-unlinking
* simple LHS conditional expressions (written in Python)

Installation
------------

Install dependencies:

    pip install django-uuidfield

Install the package:

    python setup.py build
    sudo python setup.py install

In your Django project's settings.py, add the 'rete' and 'triple' apps to your
INSTALLED_APPS list.

You can run unittests with:

    python setup.py tests
    
Usage
-----

    from rete.models import Rete, PNode
    from triple.models import T
    
    # Instantiate a new RETE network.
    rete = Rete().save()
    
    # Add a production to the network.
    p1 = models.Production.get('rule1', [
        ['?','?x','on','?y'],
        ['?','?y','left-of','?z'],
        ['?','?z','color','red'],
    ])
    rete.add_production(p1)
    
    # Add WME to the network.
    facts = [
        T('block1', 'on', 'block2'),
        T('block2', 'left-of', 'block3'),
        T('block3', 'color', 'red'),
    ]
    for fact in facts:
        rete.add_wme(fact)
    
    # Find a count of matched productions.
    pnodes = list(PNode.objects.filter(_triggered=True).order_by('id'))
    print len(list(pnodes[0].matches))

Todo
----
* left-unlinking (there's incomplete code for this, but none of my test domains
  had productions that would have been helped by this, so finishing it wasn't
  a high priority)
* negated conditions (i.e. testing for the absence of a WME)
