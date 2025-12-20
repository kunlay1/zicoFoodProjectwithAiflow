import pandas as pd

def run_transformation():

    data = pd.read_csv(r'zipco_transaction.csv')
    # remove duplicates entry

    data.drop_duplicates(inplace=True)

    # set code to go through column to replace missing values 
    numeric_cols = data.select_dtypes(include=['float64', 'int64']).columns
    for col in numeric_cols:
    #mean_value = data[col].mean()
        data.fillna({col: data[col].mean()}, inplace=True)

    

    #fill with unknown for string column
    string_cols = data.select_dtypes(include=['object']).columns
    for col in string_cols:
        data.fillna({col: 'Unknown'}, inplace=True)

    #convert date column to datetime format
    data['Date'] = pd.to_datetime(data['Date'])        

    #creating fact and dimension tables
    #create product table
    products = data[['ProductName', 'UnitPrice']].drop_duplicates().reset_index(drop=True)
    products.index.name = 'ProductID'
    products = products.reset_index()

    # create customer table
    customers = data[['CustomerName', 'CustomerAddress', 'Customer_PhoneNumber', 'CustomerEmail']].drop_duplicates().reset_index(drop=True)
    customers.index.name = 'CustomerID'
    customers = customers.reset_index()

    # staff table
    staff = data[['Staff_Name', 'Staff_Email']].drop_duplicates().reset_index(drop=True)
    staff.index.name = 'StaffID'
    staff = staff.reset_index()

    #transaction table
    transactions = data.merge(products, on=['ProductName', 'UnitPrice'], how='left') \
                    .merge(customers, on=['CustomerName', 'CustomerAddress', 'Customer_PhoneNumber', 'CustomerEmail'], how='left') \
                    .merge(staff, left_on=['Staff_Name', 'Staff_Email'], right_on=['Staff_Name', 'Staff_Email'], how='left')

    transactions.index.name = 'TransactionID'
    transactions = transactions.reset_index() \
                        .drop(columns=['ProductName', 'UnitPrice', 'CustomerName', 'CustomerAddress', 'Customer_PhoneNumber', 'CustomerEmail', 'Staff_Name', 'Staff_Email'])   

    # save data to CV
    data.to_csv('cleaned_data.csv', index=False)
    products.to_csv('products.csv', index=False)
    customers.to_csv('customers.csv', index=False)
    staff.to_csv('staff.csv', index=False)
    transactions.to_csv('transactions.csv', index=False)
    print("Data cleaning and transformation completed successfully.")