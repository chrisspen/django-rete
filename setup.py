#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
from distutils.core import setup, Command
import rete

class TestCommand(Command):
    description = "Runs unittests."
    
    user_options = [
        # e.g. --name=rete.Test.test_blah
        ('name=', 'n', 'Name of the test or test case.'),
    ]
    
    def initialize_options(self):
        self.name = 'triple rete'
    def finalize_options(self):
        pass
    def run(self, *args, **kwargs):
        name = self.name
        os.system('django-admin.py test --pythonpath=. --settings=test_settings -v 2 %s' % name)

setup(name='django-rete',
    version=rete.__version__,
    description='The RETE-UL algorithm implemented on top of Django\'s ORM.',
    author='Chris Spencer',
    author_email='chrisspen@gmail.com',
    url='https://github.com/chrisspen/django-rete',
    license='LGPL License',
    platforms=['OS Independent'],
    packages=[
        'rete',
        'triple'
    ],
    requires=['django-uuidfield'],
    cmdclass={
        'test': TestCommand,
    },
)