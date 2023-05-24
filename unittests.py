from employeeETL import name_cleaner, birthdate_cleaner, number_cleaner
import pandas as pd


def test_name_cleaner(data):
    """
    Test the name_cleaner() function.

    Args:
        data: A sample of the name string data passed into the function.

    Raises:
        AssertionError: If the name_cleaner() function does not return the expected cleaned string data.
    """

    response = name_cleaner(data)
    response = list(response)
    
    assert response == ['Carla', 'Dena', 'EllieJ']
    
    #Pass if True
    print("PASSED")


def test_birthdate_cleaner(data):
    """
    Test the birthdate_cleaner() function.

    Args:
        data: A Series of strings representing birthdates.

    Raises:
        AssertionError: If the birthdate_cleaner() function does not return the expected result to match.
    """    

    response = birthdate_cleaner(data)
    response = list(response)
    
    assert response == ['03/03/1975', '01/01/1980', '20/11/2023']
    
    print("PASSED")



def test_number_cleaner(data):
    """
    Test the number_cleaner() function.

    Args:
        data: A Series of strings representing numbers.

    Raises:
        AssertionError: If the number_cleaner() function does not return the expected result to match.

    """

    response = number_cleaner(data)
    response = list(response)

    assert response == [39400, 10000, 75000]

    print("PASSED")
    

df = pd.read_csv('employee_details.csv')

#Extract test sample
sample = df.iloc[14:17, :]

#Test the name cleaner function
name_sample = sample['FirstName']    
test_name_cleaner(name_sample)

#Test the birthdate converter function
birthdate_sample = sample['BirthDate']
test_birthdate_cleaner(birthdate_sample)

#Test the integer cleaning function
salary_sample = sample['Salary']
test_number_cleaner(salary_sample)

    
