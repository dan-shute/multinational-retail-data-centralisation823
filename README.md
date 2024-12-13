# Multinational Retail Data Centralisation

The goal of this project is to aid a multinational company into becoming more data driven. Currently, this company has sales data that is stored in several different data sources making it hard for them to access and analyse by their employees. 

This project aims to collect and cleanse their sales data into one centralised location, allowing easy access for the members of the team. Then the data can be queried from the database and supply the current team with up to date metrics for the business.

## Installation Instructions
Clone the repository and install the dependencies using requirements.txt:
```
git clone https://github.com/dan-shute/multinational-retail-data-centralisation823 
cd multinational-retail-data-centralisation823
pip install -r requirements.txt
```

## Usage Instructions
Within this project I used the following packages:
- SQLAlchemy
- Pandas
- Boto3
- Numpy
- PostgreSQL
- Tabula-py

When you follow the above Installation Instructions, you will be able to utilise the python scripts correctly.

For usage with these scripts, I recommend to import each class directly into your own script. For example, using ```from data_extraction import DataExtractor```. This will allow you to create a DataExtractor instance and utilise it's methods. 'db_creds.yaml' is a file you may have to create with your own database credentials if you wish to use this. 