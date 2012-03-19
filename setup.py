from distutils.core import setup
import rete
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
    ])
