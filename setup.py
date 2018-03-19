from setuptools import setup, find_packages

setup(
    name='sqlalchemy-jsonseeder',
    version='0.1.0',
    packages=find_packages(exclude=["tests"]),
    package_data={"jsonseeder": ["resources/*"]},
    url='https://github.com/RiceKab/sqlalchemy-jsonseeder',
    license='MIT',
    author='Kevin CY Tang',
    author_email='kevin@cyborn.be',
    keywords='sqlalchemy json seed',
    description="Tool for creating (and persisting) SQLAlchemy entities from JSON formatted data.",
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Utilities',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3'
    ],
    install_requires=['SQLAlchemy', 'jsonschema'],
    test_requires=["pytest"],
    python_requires='>=2.7'
)
