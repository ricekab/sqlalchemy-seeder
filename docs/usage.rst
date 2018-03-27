Seeders
=======
Basic seeder
------------
If you only need to create an object using a simple field->value mapping you can do so with the
:class:`~sqlalchemyseeder.basic_seeder.BasicSeeder` methods.

Resolving seeder
----------------
Once you want to be able to reference other entities you'll need to use a :class:`~sqlalchemyseeder.resolving_seeder.ResolvingSeeder`.
This allows for entity attributes to point to other entities (in case of relationships) or reference another entity's field
(for foreign keys or attributes).

This requires the seed file to be formatted in a specific way which we will detail in the next section.

.. _data-format:

Data format
***********

Currently supported formats:

* JSON
* YAML

---------------

The top structure is composed out of one or more **entity group** objects which define a target class and a data block.
The data block in turn contains one or more **entity data** blocks which then contains field-value pairs alongside
the special `!refs` key where references are defined.

The general structure is outlined here (using JSON), for some complete examples :ref:`format-examples`.

* Entity Group

Either a list of entity groups or a single entity group should form the root node of your data.

For the target class you can provide a class name (eg. `MyClass`) or a path to the class (eg. `path.to.module:MyClass`)

A single entity group:

.. code-block:: json

    {
        "target_class": "MyClass",
        "data": {}
    }

A list of entity groups:

.. code-block:: json

    [{
        "target_class": "MyClass",
        "data": {}
    },{
        "target_class": "my.module.OtherClass",
        "data": []
    }]

* Entity Data

An entity data node defines a single entity. The `!refs` field is an optional key where you can define field values as
references to other entities. These reference definitions are outlined in the next section.

A simple data block, without references, would simple be like this:

.. code-block:: json

    {
        "my_field": "my_value",
        "my_number": 123
    }

An example with references:

.. code-block:: json

    {
        "my_field": "my_value",
        "my_number": 123,
        "!refs": {
            "my_other_class": {}
        }
    }

In this example, the resolved reference is assigned to the attribute `my_other_class` of the defined entity.

* Reference Description

The reference description defines which entity is being referenced based on some provided criteria and a target class.
 Optionally, a field can be provided which corresponds to a referenced attribute of the matched entity.
 If no field is defined the entire object is used as a reference (eg. for relationships).

.. code-block:: json

    {
        "target_class": "OtherClass",
        "criteria": {
            "name": "My Name"
        }
    }

Specifying a specific field:

.. code-block:: json

    {
        "target_class": "my.module.OtherClass",
        "criteria": {
            "length": 4,
            "width": 6
        },
        "field": "name"
    }


.. _format-examples:

Format examples
***************

Examples will be built up using JSON, the final example in each section will include a YAML version.
The examples use the following model classes (in a module called "example.model"):

.. code-block:: python

    # In module example.model
    class Country(Base):
        __tablename__ = 'country'

        id = Column(Integer, primary_key=True)
        short = Column(String(5))
        name = Column(String(100))

        airports = relationship("Airport", back_populates="country")

    class Airport(Base):
        __tablename__ = 'airport'

        id = Column(Integer, primary_key=True)
        icao = Column(String(4))
        name = Column(String(100))
        altitude = Column(Integer)

        country_id = Column(Integer, ForeignKey("country.id"), nullable=False)
        country = relationship("Country", back_populates="airports")

Basic examples
..............

Let's start with defining just a single country:

.. code-block:: json

    {
        "target_class": "Country",
        "data": {
            "name": "United Kingdom",
            "short": "UK"
        }
    }

Defining multiple countries is fairly trivial as well:

.. code-block:: json

    {
        "target_class": "example.module:Country",
        "data": [
            {
                "name": "United Kingdom",
                "short": "UK"
            }, {
                "name": "Belgium",
                "short": "BE"
            }
        ]
    }

You could define them separately if preferred:

.. code-block:: json

    [
        {
            "target_class": "Country",
            "data":
                {
                    "name": "United Kingdom",
                    "short": "UK"
                }

        },
        {
            "target_class": "Country",
            "data": {
                "name": "Belgium",
                "short": "BE"
            }
        }
    ]

In yaml these would be:

.. code-block:: yaml

    --- # Compact
    target_class: example.module:Country
    data:
    - name: United Kingdom
      short: UK
    - name: Belgium
      short: BE

.. code-block:: yaml

    --- # Separate
    - target_class: Country
      data:
        name: United Kingdom
        short: UK
    - target_class: Country
      data:
        name: Belgium
        short: BE


Referencing other entities
..........................

When referencing other entities you specify a number of criteria to find the matching entity. This can use any of the
 fields that are defined in the referenced entity class.

If there is more than one match, or no matches are found an error will be thrown.

From our example model, `Airport`s require a reference to a country, either through the `country_id` foreign key or via
the `country` relationship. Here are several ways to fulfil this requirement by reference:


.. code-block:: json

    {
        "target_class": "Airport",
        "data": {
            "icao": "EGLL",
            "name": "London Heathrow",
            "!refs": {
                "country_id": {
                    "target_class": "Country",
                    "criteria": {
                        "short": "UK"
                    },
                    "field": "id"
                }
            }
        }
    }

You can also do it via the relationship:

.. code-block:: json

    {
        "target_class": "Airport",
        "data": {
            "icao": "EGLL",
            "name": "London Heathrow",
            "!refs": {
                "country": {
                    "target_class": "Country",
                    "criteria": {
                        "short": "UK"
                    }
                }
            }
        }
    }

You can also reference entities that are inserted from the same file. Here the `country` relationship in the Airport entity is
populated with the object that is created from this schema.


.. code-block:: json

    [
        {
            "target_class": "Country",
            "data":
                {
                    "name": "United Kingdom",
                    "short": "UK"
                }
        },
        {
            "target_class": "Airport",
            "data": {
                "icao": "EGLL",
                "name": "London Heathrow",
                "!refs": {
                    "country": {
                        "target_class": "Country",
                        "criteria": {
                            "short": "UK"
                        }
                    }
                }
            }
        }
    ]

This same example in yaml:

.. code-block:: yaml

    ---
    - target_class: Country
      data:
        name: United Kingdom
        short: UK
    - target_class: Airport,
      data:
        icao: EGLL
        name: London Heathrow
        '!refs':                    # <-- Due to the '!' symbol it has to be surrounded in quotes.
          country:
            target_class: Country,
            criteria:
              short: UK

Comprehensive example
.....................
Three countries each with a single airport.

.. code-block:: json

    [
      {
        "target_class": "example.module:Country",
        "data": [
          {
            "name": "United Kingdom",
            "short": "UK"
          },
          {
            "name": "Belgium",
            "short": "BE"
          },
          {
            "name": "Netherlands",
            "short": "NL"
          }
        ]
      },
      {
        "target_class": "example.module:Airport",
        "data": [
          {
            "icao": "EGLL",
            "name": "London Heathrow",
            "!refs": {
              "country": {
                "target_class": "Country,",
                "criteria": {
                  "short": "UK"
                }
              }
            }
          },
          {
            "icao": "EBBR",
            "name": "Brussels Zaventem",
            "!refs": {
              "country_id": {
                "target_class": "Country,",
                "criteria": {
                  "short": "BE"
                },
                "field": "id"
              }
            }
          },
          {
            "icao": "EHAM",
            "name": "Amsterdam Schiphol",
            "!refs": {
              "country": {
                "target_class": "Country,",
                "criteria": {
                  "name": "Netherlands"
                }
              }
            }
          }
        ]
      }
    ]

.. code-block:: yaml

    ---
    - target_class: example.module:Country
      data:
      - name: United Kingdom
        short: UK
      - name: Belgium
        short: BE
      - name: Netherlands
        short: NL
    - target_class: example.module:Airport
      data:
      - icao: EGLL
        name: London Heathrow
        '!refs':
            country:
                    target_class: Country,
            criteria:
                short: UK
      - icao: EBBR
        name: Brussels Zaventem
        '!refs':
            country_id:
                    target_class: Country,
            criteria:
                short: BE
            field: id
        - icao: EHAM
        name: Amsterdam Schiphol
        '!refs':
            country:
                    target_class: Country,
            criteria:
                name: Netherlands

Using the resolving seeder
**************************
A :class:`~sqlalchemyseeder.ResolvingSeeder` needs access to a session (provided on initialization) which it uses to resolve references.

A basic usage example:

.. code-block:: python

    from seeder import ResolvingSeeder
    from db import Session  # Or wherever you would get your session

    session = Session()
    seeder = ResolvingSeeder(session)
    # See API reference for more options
    new_entities = seeder.load_entities_from_yaml_file("path/to/file.yaml")
    session.commit()
