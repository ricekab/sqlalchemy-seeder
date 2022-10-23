# sqlalchemy-seeder

Seed SQLAlchemy database with a simple data format. Supports references to other entities (and their fields)
that are defined alongside it or persisted in the database.

## Requirements & Installation

Runs on Python 3.4 or higher.

### Dependencies

* sqlalchemy
* jsonschema
* pyyaml
    
### Installation

`pip install sqlalchemy-seeder`

## Documentation
    
http://sqlalchemy-seeder.readthedocs.io/en/latest/

## Contributing

This repository is in maintenance mode. You can submit fixes and issues via [Github Issues](https://github.com/ricekab/sqlalchemy-seeder/issues).

### Running tests

In the repository root directory: `pytest tests`

### Building docs

Requires the `sphinx` package to build. Run the following command in the repository root directory to generate the html pages in `docs/_build`:

`sphinx-build docs/ docs/_build`

### Publishing to PyPI

Using `twine`.

## License

MIT License