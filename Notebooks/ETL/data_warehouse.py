from sqlalchemy import create_engine, inspect, Column, Integer, String, Float, DateTime, FLOAT
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import database_exists, create_database


class DataWarehouse:
    '''
    Connect to Data Warehouse and create database and tables and store data there.
    '''
    def __init__(self):
        self.USER = 'postgres'
        self.PASSWORD = '1234'
        self.DATABASE_NAME = 'justdice'
        self.SQLALCHEMY_DATABASE_URL = \
            f"postgresql+psycopg2://{self.USER}:{self.PASSWORD}@localhost:5432/{self.DATABASE_NAME}"
    

    def connection(self):
        self.engine = create_engine(self.SQLALCHEMY_DATABASE_URL, echo=False)
        self.Base = declarative_base()

        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    def create_database(self):
        '''
        Create a database to collect data there and if it exists don't do anything.
        '''
        is_database_exists = database_exists(self.SQLALCHEMY_DATABASE_URL)
        if not is_database_exists:
            engine = create_engine(self.SQLALCHEMY_DATABASE_URL)
            create_database(engine.url)
            engine.dispose()
    
    ########################################################################################################
    # Tables
    
    ######################################################
    # adspend
    def adspend(self, create:bool=False):

        class Adspend(self.Base):
            __tablename__ = 'adspend'
            id = Column(Integer, primary_key=True)
            event_date = Column(DateTime, nullable=False)
            country_id = Column(String(10), nullable=False)
            network_id = Column(String(10), nullable=False)
            client_id = Column(String(10), nullable=False)
            value_usd = Column(Float, nullable=False)
        if create:
            self.Base.metadata.create_all(self.engine)

        return Adspend

    def store_adspend(self, table, item):
        new_record = table(
            event_date = item['event_date'],
            country_id = item['country_id'],
            network_id = item['network_id'],
            client_id = item['client_id'],
            value_usd = item['value_usd']
            )
        self.session.add(new_record)
        self.session.commit()

    ######################################################
    # installs
    def installs(self, create:bool=False):
        class Installs(self.Base):
            __tablename__ = 'installs'
            id = Column(Integer, primary_key=True)
            install_id = Column(String(100), nullable=False)
            country_id = Column(String(10), nullable=False)
            app_id = Column(String(10), nullable=False)
            network_id = Column(String(10), nullable=False)
            event_date = Column(DateTime, nullable=False)
            device_os_version = Column(String(100), nullable=False)

        if create:
            self.Base.metadata.create_all(self.engine)

        return Installs


    def store_installs(self, table, item):
        new_record = table(
            install_id = item['install_id'],
            country_id = item['country_id'],
            app_id = item['app_id'],
            network_id = item['network_id'],
            event_date = item['event_date'],
            device_os_version = item['device_os_version'],
            )
        self.session.add(new_record)
        self.session.commit()
    
    ######################################################
    # payouts
    def payouts(self, create:bool=False):
        class Payouts(self.Base):
            __tablename__ = 'payouts'
            id = Column(Integer, primary_key=True)
            install_id = Column(String(100), nullable=False)
            event_date = Column(DateTime, nullable=False)
            value_usd = Column(Float, nullable=False)

        if create:
            self.Base.metadata.create_all(self.engine)

        return Payouts


    def store_payouts(self, table, item):
        new_record = table(
            install_id = item['install_id'],
            event_date = item['event_date'],
            value_usd = item['value_usd'],
            )
        self.session.add(new_record)
        self.session.commit()

    ######################################################
    #revenue
    def revenue(self, create:bool=False):
        class Revenue(self.Base):
            __tablename__ = 'revenue'
            id = Column(Integer, primary_key=True)
            install_id = Column(String(100), nullable=False)
            event_date = Column(DateTime, nullable=False)
            value_usd = Column(Float, nullable=False)

        if create:
            self.Base.metadata.create_all(self.engine)

        return Revenue


    def store_revenue(self, table, item):
        new_record = table(
            install_id = item['install_id'],
            event_date = item['event_date'],
            value_usd = item['value_usd'],
            )
        self.session.add(new_record)
        self.session.commit()