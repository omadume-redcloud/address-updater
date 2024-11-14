import csv

db_name = '' # Set this to the appropriate DB
address_data_csv_path = '' # The customer address info, exported from an Excel worksheet
select_statement_sql_path = 'select_statement.sql' # Where the file containing the SQL SELECT statement will be generated
customer_data_csv_path = '' # The customer data results of the SELECT query, exported from MySQL workbench
update_transaction_sql_path = 'transaction.sql' # # Where the file containing the SQL update transaction will be generated

# Generate the SELECT statement for fetching the existing customer details - to be used for creating new customer address records
def generate_customer_data_select_query():
    # Open the CSV input and output SQL files
    with open(address_data_csv_path, mode='r', newline='', encoding='utf-8-sig') as csv_file, open(select_statement_sql_path, mode='w', newline='', encoding='utf-8') as sql_file:
        reader = csv.DictReader(csv_file, delimiter=';') # Delimiter is ; instead of , for Portugal PC
        buyer_ids = set() # There are multiple entries for some customer ids in the csv
        
        # print(reader.fieldnames) # To check the column names retrieved from the csv file

        for row in reader:
            buyer_id = row['Buyer ID'.strip()]
            buyer_ids.add(buyer_id)

        buyer_ids_str = ', '.join(map(str, buyer_ids))

        sql_file.write(f"-- Fetching customer details for Buyer IDs\n")
        sql_file.write(f"SELECT entity_id, firstname, lastname, middlename, phone_number FROM {db_name}.customer_entity WHERE entity_id IN ({buyer_ids_str});\n") # These values cannot be null when creating new customer address records

    print(f"SQL script generated and saved to {select_statement_sql_path}")

# Return NULL if the value is 'NULL' or an empty string, else return it wrapped in single quotes
def check_for_null(value):
    if value == "NULL" or value == "":
        return "NULL"
    else:
        return f"'{value}'"

# Generate the SQL transaction for performing the customer addresses update
def generate_address_update_transaction():
    # Open the CSV input and output SQL files
    with open(customer_data_csv_path, mode='r', newline='', encoding='utf-8-sig') as customer_data_csv_file, \
        open(address_data_csv_path, mode='r', newline='', encoding='utf-8-sig') as address_data_csv_file, \
        open(update_transaction_sql_path, mode='w', newline='', encoding='utf-8-sig') as sql_file:

        # Read all of the customer data into a dict
        reader1 = csv.DictReader(customer_data_csv_file, delimiter=',') # Delimiter is , on MySQL Workbench, change if necessary
        customer_data = {}

        for row in reader1:
            buyer_id = check_for_null(row['entity_id'])
            customer_data[buyer_id] = {
                'firstname': check_for_null(row['firstname']),
                'lastname': check_for_null(row['lastname']),
                'middlename': check_for_null(row['middlename']),
                'phone_number': check_for_null(row['phone_number'])
            }
        
        # Read csv address data - to combine with customer data when creating new address records below
        reader2 = csv.DictReader(address_data_csv_file, delimiter=';') # Delimiter is ; instead of , for Portugal PC
        unique_customers = set() # There are some customers with multiple addresses in the csv, but we only want to use their first one
        
        # Write the SQL transaction for updating addresses, specifying the transaction security level to facilitate being ACID
        sql_file.write("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;\n\n") # Isolation level wouldn't change in MySQL workbench without setting for entire session instead of just transaction

        sql_file.write("START TRANSACTION;\n\n")
        
        try:
            for row in reader2:
                buyer_id = check_for_null(row['Buyer ID'.strip()])
                street = check_for_null(row['Street Number'.strip()])
                city = check_for_null(row['City'.strip()])
                postcode = check_for_null(row['Postal Code'.strip()])
                country_id = 'AR' # Argentina
                
                if buyer_id not in unique_customers and buyer_id in customer_data.keys():
                    unique_customers.add(buyer_id)
                    
                    # Create new address record, combining the customer data and address data
                    sql_file.write(f"-- Inserting new address for Buyer ID: {buyer_id}\n")
                    customer = customer_data[buyer_id]
                    sql_file.write(f"INSERT INTO {db_name}.customer_address_entity (parent_id, street, city, postcode, country_id, firstname, lastname, middlename, telephone) "
                                   f"VALUES ({buyer_id}, {street}, {city}, {postcode}, '{country_id}', "
                                   f"{customer['firstname']}, {customer['lastname']}, {customer['middlename']}, {customer['phone_number']});\n\n")
                    
                    # Get the ID of the newly-created address record
                    sql_file.write(f"SET @new_address_id = LAST_INSERT_ID();\n\n")
                    
                    # Set new address as default billing and shipping address for the customer
                    sql_file.write(f"-- Setting new address as default billing and shipping for Buyer ID: {buyer_id}\n")
                    sql_file.write(f"UPDATE {db_name}.customer_entity SET default_billing = @new_address_id, default_shipping = @new_address_id WHERE entity_id = {buyer_id};\n\n")
                    
                    # Delete other addresses for this customer - Wesley said to leave this for now
                    # sql_file.write(f"-- Deleting non-default addresses for Buyer ID: {buyer_id}\n")
                    # sql_file.write(f"DELETE FROM {db_name}.customer_address_entity WHERE parent_id = {buyer_id} AND entity_id != @new_address_id;\n\n")
            
            # Commit the transaction
            sql_file.write("COMMIT;\n")

        except Exception as e:
            # Rollback the transaction in case of any error
            sql_file.write("ROLLBACK;\n")
            print("An error occurred during script generation:", e)
        
    print(f"SQL script generated and saved to {update_transaction_sql_path}")


# generate_customer_data_select_query() # Run this first
# generate_address_update_transaction()