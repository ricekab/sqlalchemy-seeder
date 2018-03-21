# sqlalchemy-jsonseeder
Seed SQLAlchemy database with JSON formatted data. Supports references to other entities (and their fields)
that are defined alongside it or persisted in the database.

## Requirements & Installation

Runs on Python 2.7 or Python 3

- Dependencies
    * sqlalchemy
    * jsonschema
    
* Installation
`pip install sqlalchemy-jsonseeder`

## Usage
Currently there are 2 seeders available: `BasicSeeder` and `ResolvingSeeder`.

`BasicSeeder` only provides a simple static method for converting a dictionary / json string to an entity object.
It does not perform any logic to validate or resolve the values. Wrong values will cause a `KeyError`.

`ResolvingSeeder` allows you to define multiple entities in one file as well as define referential values.
This requires some special JSON format so the seeder will know how to resolve them.

ResolvingSeeder requires a session to be provided that it uses to query the database (and flush/commit as required).

Since it has to be made aware of classes

### JSON Structure

The top structure is composed out of one or more `entity group` objects which define a target class and a data block.
The data block in turn contains one or more `entity data` blocks which then contains simple key-value pairs alongside 
the special `!refs` key where references are defined.
 
The general structure is outlined here, for some complete examples see further below.

* Entity Group

Either a list of entity groups or a single entity group should form the root node of your json data.

For the target class you can provide a class name (eg. `MyClass`) or a path to the class (eg. `path.to.module:MyClass`) 



    {
        "target_class": <class_name> or <class_path>,
        "data": list of <entity_data> objects or a single <entity_data> object 
    }
        
    
* Entity Data

Defines a single entity. The `!refs` field is a special keyword that defines references to other entities.
It is optional.


    {
        "my_field": "my_value",
        "my_number": 123,
        ...
        "!refs": {
            "my_reference_relation": <reference_description>,
            ...
        }
    }
    
* Reference Description

The reference description defines which entity is being referenced based on some provided criteria. Optionally,
 a field can be provided which will be the referenced attribute of the matched entity. If no field is defined
 the entire object is used as a reference (for relationships).


    {
        "target_class": <class_name> or <class_path>,
        "criteria": {
            "referenced_field": "required_value",
            ..
        },
        "field": <referenced_entity_field>              <-- If missing or empty this reference uses the entity itself.
    }

#### Examples
For the examples we are using these model classes (in a module called "example.model"):

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

1. Basic example Single country

This is the simplest form of input.
    
    {
        "target_class": "Country",
        "data": {
            "name": "United Kingdom",
            "short": "UK"
        }
    }


2. Multiple countries

You can define these together (more compact):


    {
        "target_class": "example.module:Country",        <-- Class path example
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
    
Or separate if preferred:

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
    
3. Referencing other entities

Here the defined airport specifies that the value of `country_id` references a country class's `id` field 
where its field `short` is `"UK"`. 
If there is more than one match, or no matches are found an error will be thrown.


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

You can also reference entities that are inserted from the same file. Here the `country` relationship in the Airport entity is
populated with the object that is created from this schema.



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
    
    

## Issues
Submit issues on [Github Issues](https://github.com/RiceKab/sqlalchemy-jsonseeder/issues).
## License
MIT License