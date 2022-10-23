from setuptools import setup, find_packages

setup(
    name='sqlalchemy-seeder',
    version='0.3.1',
    packages=find_packages(exclude=["tests"]),
    package_data={"sqlalchemyseeder": ["resources/*"]},
    url='https://github.com/ricekab/sqlalchemy-seeder',
    license='MIT',
    author='Kevin CY Tang',
    author_email='kevin@cyborn.be',
    keywords='sqlalchemy json yaml seed',
    description="Tool for creating (and persisting) SQLAlchemy entities from a simple data format.",
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Utilities',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3'
    ],
    install_requires=['SQLAlchemy', 'jsonschema', 'pyyaml'],
    tests_require=["pytest"],
    python_requires='>=3.4'
)
