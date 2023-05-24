import pandas as pd
import re
import numpy as np
from datetime import datetime
from pymongo import MongoClient
from pymongo.errors import BulkWriteError
import logging

logging.basicConfig(level=logging.INFO, filename='ETLlog.log', filemode='w',
                    format="%(asctime)s - %(levelname)s - %(message)s"
                    )

def read_csv(file_path):
    """
    Read a CSV file into a Pandas DataFrame.

    Parameters
    ----------
    file_path : str
        Path to the CSV file.

    Returns
    -------
    pd.DataFrame
        Pandas DataFrame containing the data from the CSV file.

    """
    logging.info('Reading CSV to Pandas dataframe')
    return pd.read_csv(file_path)

def name_cleaner(name_column):
    """
    Clean a column of names by removing whitespace and non-alphabetical characters.

    Parameters
    ----------
    name_column : pd.Series
        Column of names to be cleaned.

    Returns
    -------
    pd.Series
        Cleaned column of names.

    """

    clean_column = name_column.replace('\s', '')
    clean_column = clean_column.apply(lambda x : re.sub('[^a-zA-ZÀ-ÿ]+', '', x))

    return clean_column

def birthdate_cleaner(birth_date_column):
    """
    Clean a column of birthdates by converting them to the format DD/MM/YYYY and replacing any missing values with 01/01/2023.

    Parameters
    ----------
    birth_date_column : pd.Series
        Column of birthdates to be cleaned.

    Returns
    -------
    pd.Series
        Cleaned column of birthdates.
    """

    format = '%Y/%m/%d' #for current format of 1990-06-12
    birth_date_column = birth_date_column.apply(lambda x : pd.to_datetime(x, format=format, errors='coerce'))

    #Convert the BirthDate from the format YYYY-MM-DD to DD/MM/YYYY
    birth_date_column = [x.strftime('%d/%m/%Y') if not pd.isnull(x) else '01/01/2023' for x in birth_date_column]

    return birth_date_column

def create_fullName(dataframe):
    """
    Create a full name column from the `FirstName Cleaned` and `LastName` columns of a Pandas DataFrame.

    Parameters
    ----------
    dataframe : pd.DataFrame
        DataFrame containing the `FirstName Cleaned` and `LastName` columns.

    Returns
    -------
    pd.Series
        Column of full names.

    """
    #Split strings if they contain another capitalised letter- assume it is an inital
    uppercase_split = dataframe['FirstName'].apply(lambda x: re.sub(r"([A-Z])", r" \1", x).split())
    #Produce list of forenames
    firstnames = [' '.join(parts) for parts in uppercase_split]
    dataframe['FirstName Cleaned'] = firstnames
    dataframe.drop(columns = ['FirstName'], inplace = True)

    #Create full name column
    dataframe['FullName'] = dataframe['FirstName Cleaned'].fillna(dataframe['LastName']) + ' ' + dataframe['LastName'].fillna(dataframe['FirstName Cleaned'])
    #Whitespace can be introduced where blank name so remove
    dataframe['FullName'] = dataframe['FullName'].str.strip()

    return dataframe['FullName']


def calculate_age(birthdays, rel_date):
    """
    Calculate the age of a person based on their birthday and a relative date of 1st day of 2023.

    Parameters
    ----------
    birthdays : pd.Series
        Column of birthdays.
    rel_date : str
        Relative date in the format DD/MM/YYYY.

    Returns
    -------
    pd.Series
        Column of age data.
    """

    #Convert relative date time timestamp
    rel_date_datetime = datetime.strptime(rel_date, '%d/%m/%Y')
    #And convert column to timestamp
    birthdays_datetime = pd.to_datetime(birthdays)
    #Get the number of days in difference
    differences = birthdays_datetime - rel_date_datetime
    #Convert days to time in years
    ages = [abs(int(day_count.days/365)) for day_count in differences]
    return ages


def number_cleaner(number_column):
    """
    Clean a column of numbers by removing letters, filling in missing values with 0, and converting the values to integers.

    Parameters
    ----------
    number_column : pd.Series
        Column of numbers to be cleaned.

    Returns
    -------
    pd.Series
        Cleaned column of numbers.
    """

    number_column.fillna('0', inplace = True)
    #Remove letters
    number_column_cleaned = number_column.apply(lambda x : re.sub('[a-zA-ZÀ-ÿ]+', '', x))
    #Keep absolute value, no negative salaries
    number_column_cleaned = number_column_cleaned.apply(lambda x : abs(int(x)))
    
    return number_column_cleaned


def salaryBucketer(row):
    """
    Bucket a salary into one of three categories: A, B, or C.

    Parameters
    ----------
    row : pd.Series
        Row of the dataframe containing the salary information.

    Returns
    -------
    str
        Salary bucket.

    """

    if (row['Salary'] > 0) & (row['Salary'] < 50000):
        result = 'A'
    elif (50000 <= row['Salary']) & (row['Salary'] <= 100000):
        result = 'B'
    elif row['Salary'] > 100000:
        result = 'C'
    else:
        result = ''
    
    return result    


def transform_data(dataframe):
    """
    Transforms a dataframe by cleaning the data and creating new columns.

    Parameters
    ----------
    dataframe : pd.DataFrame
        Dataframe to be transformed.

    Returns
    -------
    pd.DataFrame
        Transformed dataframe.
    """

    #Drop row values that are the column names
    for i,v in dataframe.iterrows():
        if list(v.values) == list(dataframe.columns):
            dataframe.drop(index = i, inplace = True) 

    #Remove whitespace in column names
    dataframe.columns = dataframe.columns.str.strip()

    #Clean ID and Department which have whitespace
    dataframe['EmployeeID'], dataframe['Department'] = dataframe['EmployeeID'].str.replace('\s', ''), dataframe['Department'].str.replace('\s', '')

    #Apply fix to row that needs partial shifting by indexing
    dataframe.iloc[29, 3], dataframe.iloc[29, 5] = dataframe.iloc[29, 2], dataframe.iloc[29, 4]
    dataframe.iloc[29, 2], dataframe.iloc[29, 4] = '', ''

    #Clean birth date column and make datetime
    dataframe['BirthDate'] = birthdate_cleaner(dataframe['BirthDate'])
    logging.info('Cleaning complete')

    #Clean name columns
    dataframe['FirstName'] = name_cleaner(dataframe['FirstName'])
    dataframe['LastName'] = name_cleaner(dataframe['LastName'])

    #Create full name column
    dataframe['Full Name'] = create_fullName(dataframe)
    logging.info('Full name column created')

    #Create age column based off of birth dates
    dataframe['Age'] = calculate_age(dataframe['BirthDate'], '01/01/2023')
    logging.info('Employee age column created')

    #Clean salary column
    dataframe['Salary'] = number_cleaner(dataframe['Salary'])

    #Apply bucketer function to salary
    dataframe['SalaryBucket'] = dataframe.apply(salaryBucketer, axis=1)
    logging.info('Salary bucket column created')

    #Drop transformed columns
    dataframe.drop(columns = ['FirstName Cleaned', 'LastName', 'BirthDate'], inplace = True)

    return dataframe


def load_data(mongo_credentials, df):
    """
    Connects to MongoDB client and loads a Pandas DataFrame into a MongoDB collection.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame to be loaded.

    Raises: BulkWriteError if client fails to upload the collection to the database.
    """

    #Retrieve the mongo credentials
    credentials_file = open(mongo_credentials)
    mongo_token = credentials_file.read()
    credentials_file.close()
    logging.info('MongoDB credentials obtained')

    #Use client to interact with cluster hosted on Mongo Atlas
    cluster = MongoClient(mongo_token)
    logging.info('Connected to MongoDB cloud cluster')
    db = cluster["employee_db"]
    collection = db['employee']

    df.rename(columns = {'EmployeeID' : '_id'}, inplace = True)

    #Post collection to the database
    try:
        db.collection.insert_many(df.to_dict('records'))
        logging.info('Collection uploaded to MongoDB')
    except BulkWriteError as bwe:
        logging.error("BulkWriteError", exc_info=True)