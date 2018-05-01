import pytest
from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, DECIMAL, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker

TestBase = declarative_base()


class User(TestBase):
    __tablename__ = 'user'

    id = Column(Integer, primary_key=True)
    name = Column(String(50), nullable=False)
    date_of_birth = Column(DateTime, nullable=True)

    country_id = Column(Integer, ForeignKey('country.id'), nullable=True)

    addresses = relationship("Address", backref="user")


class Address(TestBase):
    __tablename__ = 'address'

    id = Column(Integer, primary_key=True)
    email = Column(String(50), nullable=False)
    user_id = Column(Integer, ForeignKey('user.id'), nullable=True)


class Country(TestBase):
    __tablename__ = 'country'

    id = Column(Integer, primary_key=True)
    short = Column(String(5))
    name = Column(String(100))

    airports = relationship("Airport", back_populates="country")
    users = relationship("User", backref="country")


class Airport(TestBase):
    __tablename__ = 'airport'

    id = Column(Integer, primary_key=True)
    icao = Column(String(4))
    name = Column(String(100))
    latitude = Column(DECIMAL)
    longitude = Column(DECIMAL)
    altitude = Column(Integer)

    country_id = Column(Integer, ForeignKey("country.id"), nullable=False)
    country = relationship("Country", back_populates="airports")


class Models(object):
    def __init__(self):
        self.TestBase = TestBase
        self.User = User
        self.Address = Address
        self.Country = Country
        self.Airport = Airport


_models = Models()
_engine = create_engine("sqlite://")
TestBase.metadata.bind = _engine


@pytest.fixture(autouse=True)
def clean_db():
    TestBase.metadata.drop_all(_engine)
    TestBase.metadata.create_all(_engine)


@pytest.fixture(scope='session')
def model():
    return _models


@pytest.fixture(scope='session')
def session_factory():
    return sessionmaker(bind=_engine)


@pytest.fixture()
def session(session_factory):
    return session_factory()
