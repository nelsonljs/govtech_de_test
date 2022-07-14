from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import validates, Session
from sqlalchemy.sql import *
import random
import os

from datetime import datetime, timedelta
from dotenv import load_dotenv
load_dotenv()

Base = declarative_base()   

class DimCar(Base):
    __tablename__ = "dim_cars"
    id = Column(Integer, primary_key=True)
    manufacturer = Column(String)
    model_name = Column(String)
    serial_number = Column(String, unique=True)
    weight = Column(Numeric)

    ### Validation for a car and other dimensions can be specified.
    # @validates('manufacturer')
    # def validate_dob(self, key, value):
    #     if value not in ['Honda','Toyota']:
    #         raise ValueError("Manufacturer not recognized.")
    #     return value

class DimDate(Base):
    '''
    This could be imported from pre-constructed data-lake in use in the enterprise. Otherwise, for simplicity only capturing basic info.
    '''
    __tablename__ = "dim_dates"
    id = Column(Integer, primary_key=True, autoincrement=True)
    date_year = Column(Integer)
    date_month = Column(Integer)
    date_day = Column(Integer)

class DimSalesperson(Base):
    __tablename__ = "dim_salesperson"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String)
    position = Column(String)
    employed_date = Column(DateTime)

class DimCustomer(Base):
    __tablename__ = "dim_customers"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String)
    phone = Column(String)
    registered_date = Column(DateTime)

class FactSales(Base):
    __tablename__ = "fct_sales"
    id = Column(Integer, primary_key=True)
    customer_id = Column(Integer, ForeignKey('dim_customer.id'))
    salesperson_id = Column(Integer, ForeignKey('dim_salesperson.id'))
    productcar_id = Column(Integer, ForeignKey('dim_cars.id'))
    sales_date_id = Column(Integer, ForeignKey('dim_dates.id'))
    sales_price = Column(Numeric)


def main():
    '''
    Create database.
    '''
    postgres_pw = os.environ.get("POSTGRES_PASSWORD")
    postgres_db = os.environ.get("POSTGRES_DB")
    docker_container_name = os.environ.get("POSTGRES_CONTAINER")
    # engine = create_engine(f'sqlite:///data/{db_name}.db')
    engine = create_engine(f'postgresql://postgres:{postgres_pw}@{docker_container_name}:5432/{postgres_db}')

    Base.metadata.create_all(bind=engine)

    print('Tables created.')

    ## Setting up dimdates table for BI requirements.
    with Session(engine) as s:
        mydate = datetime(2020,1,1)
        while mydate < datetime.today():
            s.add(
                DimDate(
                    id=int(mydate.strftime('%Y%m%d')),
                    date_year=mydate.year,
                    date_month=mydate.month,
                    date_day=mydate.day
                )
            )
            mydate += timedelta(days=1)
        
        s.commit()

if __name__ == '__main__':
    main()
