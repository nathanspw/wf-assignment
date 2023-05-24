from employeeETL import *

def main():
    df = read_csv('employee_details.csv')

    #Use the ETL function
    df = transform_data(df)

    #Use function to load data to the Mongo DB
    load_data('uri_mongo.txt', df)


if __name__ == "__main__":
    main()
