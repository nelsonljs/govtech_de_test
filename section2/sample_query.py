'''
Sample queries as requested by team. POC
'''

from model import *
from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import validates, Session
from sqlalchemy.sql import *
import pandas as pd
from dotenv import load_dotenv
load_dotenv()

postgres_pw = os.environ.get("POSTGRES_PASSWORD")
postgres_db = os.environ.get("POSTGRES_DB")
docker_container_name = os.environ.get("POSTGRES_CONTAINER")
# engine = create_engine(f'sqlite:///data/demo.db')
engine = create_engine(f'postgresql://postgres:{postgres_pw}@{docker_container_name}:5432/{postgres_db}')

with Session(engine) as s:

# 1) I want to know the list of our customers and their spending.
    temp_cte = select(func.sum(FactSales.sales_price).label('SumofSales'), FactSales.customer_id).\
        group_by(FactSales.customer_id).\
            cte('temp1')

    query1 = select(temp_cte, DimCustomer.name, DimCustomer.phone).\
        join(DimCustomer).select_from(temp_cte)

    print(query1)

    mydf = pd.read_sql(query1, con=s.bind)

# 2) I want to find out the top 3 car manufacturers that customers bought by sales (quantity) and the sales number for it in the current month.

    query2 = select(func.sum(FactSales.sales_price).label('SumofSales'), DimDate.date_month, DimCar.manufacturer).\
        join(DimDate, FactSales.sales_date_id==DimDate.id).select_from(FactSales).\
        join(DimCar).select_from(FactSales).\
            filter(DimDate.date_month==5).\
                group_by(DimDate.date_month, DimCar.manufacturer).\
                    order_by(desc('SumofSales')).\
                        limit(3)

    print(query2)

    mydf = pd.read_sql(query2, con=s.bind)

print(mydf.head(5))