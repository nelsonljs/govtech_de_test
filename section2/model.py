from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import validates, Session
from sqlalchemy.sql import *
import pandas as pd
import random

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

Base = declarative_base()   

class DimCar(Base):
    __tablename__ = "dim_cars"
    id = Column(Integer, primary_key=True)
    manufacturer = Column(String)
    model_name = Column(String)
    serial_number = Column(String)
    weight = Column(Numeric)

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
    __tablename__ = "dim_customer"
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
    sales_price = Column(Integer)


def main(db_name = 'demo'):
    '''
    Create sample sqlite database.
    '''
    engine = create_engine(f'sqlite:///data/{db_name}.db')
    Base.metadata.create_all(bind=engine)

    print('created.')

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

        s.add(DimCustomer(name='Harry',phone=123456,registered_date=datetime(2021,5,12)))
        s.add(DimCustomer(name='Ron',phone=12345,registered_date=datetime(2021,6,12)))
        s.add(DimCustomer(name='Hermione',phone=1234,registered_date=datetime(2021,7,12)))
        s.add(DimCustomer(name='Hagrid',phone=12356,registered_date=datetime(2021,8,12)))

        s.commit()

        s.add(FactSales(customer_id=1,salesperson_id=1,productcar_id=1,sales_date_id='20210513',sales_price=random.randrange(100000,150000)))
        s.add(FactSales(customer_id=1,salesperson_id=1,productcar_id=1,sales_date_id='20210113',sales_price=random.randrange(100000,150000)))
        s.add(FactSales(customer_id=2,salesperson_id=1,productcar_id=1,sales_date_id='20210213',sales_price=random.randrange(100000,150000)))
        s.add(FactSales(customer_id=2,salesperson_id=1,productcar_id=1,sales_date_id='20210513',sales_price=random.randrange(100000,150000)))
        s.add(FactSales(customer_id=2,salesperson_id=1,productcar_id=1,sales_date_id='20210513',sales_price=random.randrange(100000,150000)))
        s.add(FactSales(customer_id=2,salesperson_id=1,productcar_id=1,sales_date_id='20210513',sales_price=random.randrange(100000,150000)))
        s.add(FactSales(customer_id=3,salesperson_id=1,productcar_id=1,sales_date_id='20210613',sales_price=random.randrange(100000,150000)))
        s.add(FactSales(customer_id=4,salesperson_id=1,productcar_id=1,sales_date_id='20210113',sales_price=random.randrange(100000,150000)))
        s.add(FactSales(customer_id=4,salesperson_id=1,productcar_id=1,sales_date_id='20210113',sales_price=random.randrange(100000,150000)))
        s.add(FactSales(customer_id=4,salesperson_id=1,productcar_id=1,sales_date_id='20210213',sales_price=random.randrange(100000,150000)))
        s.add(FactSales(customer_id=4,salesperson_id=1,productcar_id=1,sales_date_id='20210513',sales_price=random.randrange(100000,150000)))

        s.commit()

if __name__ == '__main__':
    main()
