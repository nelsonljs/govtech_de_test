## Section 2

You are appointed by a car dealership to create their database infrastructure. There is only one store. In each business day, cars are being sold by a team of salespersons. Each transaction would contain information on the date and time of transaction, customer transacted with, and the car that was sold. 

The following are known:
- Each car can only be sold by one salesperson.
- There are multiple manufacturers’ cars sold.
- Each car has the following characteristics:
- Manufacturer
- Model name
- Serial number
- Weight
- Price

Each sale transaction contains the following information:
- Customer Name
- Customer Phone
- Salesperson
- Characteristics of car sold

## Data model

- Assumptions:
    - Cars can have a different selling price at different sale prices.
    - Cars will always have the same manufacturer, model name, serial number, and weight.
    - A dim_date table at day level is necessary.

- Dimension Tables:
    - Salesperson
    - Date
    - Customer
    - Cars
    - Manufacturer info??

- Fact Tables:
    - Sale transaction

## ETL Strategy

Data is to be received as Transactional Facts with the following columns:

| Column | Desc |
|---|---|
| Customer Name ||
| Customer Phone||
| Salesperson Name? | Assume unique, can be id |
| Car characteristics | Assume consistent, can be used to build dimensions on ingestion |

Build ORM representation for dialect agnostic, test locally on sqlite db.
