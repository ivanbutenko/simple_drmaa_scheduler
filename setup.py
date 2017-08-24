from setuptools import setup

setup(
    name='simple_drmaa_scheduler',
    version='0.0.1',
    packages=['scheduler', 'scheduler.parser'],
    url='',
    license='',
    author='nikita',
    author_email='',
    description='',
    entry_points={
        'console_scripts': [
            'scheduler = scheduler.cli:main'
        ]
    }, install_requires=['drmaa', 'PyYAML', 'ujson']
)
