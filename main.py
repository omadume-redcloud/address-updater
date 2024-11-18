import csv

db_name = '' # Set this to the appropriate DB
address_data_csv_path = '' # The customer address info, exported from an Excel worksheet
select_statement_sql_path = 'select_statement.sql' # Where the file containing the SQL SELECT statement will be generated
customer_data_csv_path = '' # The customer data results of the SELECT query, exported from MySQL workbench
update_transaction_sql_path = 'transaction.sql' # Where the file containing the SQL update transaction will be generated
customer_ids_list_path = 'customer_ids.csv' # File containing a list of ids for the customers that were found in the DB

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
        sql_file.write(f"SELECT entity_id, firstname, lastname, phone_number FROM {db_name}.customer_entity WHERE entity_id IN ({buyer_ids_str});\n") # Maintaining consistency of these values when creating new customer address records (also cannot be null)

    print(f"SQL script generated and saved to {select_statement_sql_path}")

# Return NULL if the value is 'NULL' or an empty string, else return it wrapped in single quotes
def check_for_null(value):
    if value == "NULL" or value == "":
        return "NULL"
    else:
        return f'"{value}"' # The values will be inserted using double quotes as some values (like names) contain single quotes

# Generate the SQL transaction for performing the customer addresses update
def generate_address_update_transaction():
    # Open the CSV input and output SQL files
    with open(customer_data_csv_path, mode='r', newline='', encoding='utf-8-sig') as customer_data_csv_file, \
        open(address_data_csv_path, mode='r', newline='', encoding='utf-8-sig') as address_data_csv_file, \
        open(update_transaction_sql_path, mode='w', newline='', encoding='utf-8-sig') as sql_file, \
        open(customer_ids_list_path, mode='w', newline='', encoding='utf-8-sig') as customer_ids_file:

        # Read all of the customer data into a dict
        reader1 = csv.DictReader(customer_data_csv_file, delimiter=',') # Delimiter is , on MySQL Workbench, change if necessary!
        customer_data = {}

        for row in reader1:
            buyer_id = row['entity_id']
            customer_data[buyer_id] = {
                'firstname': check_for_null(row['firstname']),
                'lastname': check_for_null(row['lastname']),
                'phone_number': check_for_null(row['phone_number'])
            }

        customer_ids_file.write(f"CUSTOMER ENTITY IDS:\n")
        customer_ids_file.write(f"{', '.join(customer_data.keys())}\n\n") # Saving customer ids for additional scripts - backup, rollback, etc
                
        # Write the SQL transaction for updating addresses, specifying the transaction security level to facilitate being ACID
        sql_file.write("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;\n\n") # Isolation level wouldn't change in MySQL workbench without setting for entire session instead of just transaction
        sql_file.write("START TRANSACTION;\n\n")
        
        try:
            # Read csv address data - to combine with customer data when creating new address records below
            reader2 = csv.DictReader(address_data_csv_file, delimiter=';') # Delimiter is ; instead of , for Portugal PC
            unique_customers = set() # There are some customers with multiple addresses in the csv, but we only want to use their first one

            sql_file.write(f"-- Creating table for storing entity ids of all newly-created address records (for rollback scripts, etc)\n")
            sql_file.write(f"CREATE TABLE {db_name}.new_address_ids (id INT AUTO_INCREMENT PRIMARY KEY, address_entity_id INT NOT NULL);\n\n")

            for row in reader2:
                buyer_id = row['Buyer ID'.strip()]
                street = check_for_null(row['Street Number'.strip()])
                city = check_for_null(row['City'.strip()])
                postcode = check_for_null(row['Postal Code'.strip()])
                country_id = 'AR' # Argentina
                
                if buyer_id not in unique_customers and buyer_id in customer_data.keys():
                    unique_customers.add(buyer_id)
                    
                    # Create new address record, combining the customer data and address data
                    customer = customer_data[buyer_id]
                    telephone = customer['phone_number']
                    if telephone == "NULL": # Telephone cannot not be null for an address record, otherwise transaction will fail
                        telephone = check_for_null(row['Buyer Phone Number'.strip()]) # Attempting to use excel one instead of existing DB customer record one
                    
                    sql_file.write(f"-- Inserting new address for Buyer ID: {buyer_id}\n")
                    sql_file.write(f'INSERT INTO {db_name}.customer_address_entity (parent_id, street, city, postcode, country_id, firstname, lastname, telephone) '
                                   f'VALUES ({buyer_id}, {street}, {city}, {postcode}, "{country_id}", '
                                   f'{customer["firstname"]}, {customer["lastname"]}, {telephone});\n\n') # The values will be inserted using double quotes as some values (like names) contain single quotes
                    
                    # Get the ID of the newly-created address record
                    sql_file.write(f"SET @new_address_id = LAST_INSERT_ID();\n\n")

                    # Add new address record id to separate table for keepsake
                    sql_file.write(f"-- Saving id of newly-created address record to separate DB table (for rollback scripts, etc)\n")
                    sql_file.write(f"INSERT INTO {db_name}.new_address_ids (address_entity_id) VALUES (@new_address_id);\n\n")
                    
                    # Set new address as default billing and shipping address for the customer
                    sql_file.write(f"-- Setting new address as default billing and shipping for Buyer ID: {buyer_id}\n")
                    sql_file.write(f"UPDATE {db_name}.customer_entity SET default_billing = @new_address_id, default_shipping = @new_address_id WHERE entity_id = {buyer_id};\n\n")
                    
                    # Delete other addresses for this customer - Wesley said to leave this for now. If used remember to update 'additional_scripts' file with rollback & validation for this!
                    # sql_file.write(f"-- Deleting non-default addresses for Buyer ID: {buyer_id}\n")
                    # sql_file.write(f"DELETE FROM {db_name}.customer_address_entity WHERE parent_id = {buyer_id} AND entity_id != @new_address_id;\n\n")
            
            # Commit or rollback the transaction
            sql_file.write("-- If no errors, uncomment and execute the COMMIT line below. Else, uncomment and execute ROLLBACK!\n")
            sql_file.write("-- COMMIT;\n")
            sql_file.write("-- ROLLBACK;\n")

        except Exception as e:
            print("An error occurred during script generation:", e)
        
    print(f"SQL script generated and saved to {update_transaction_sql_path}")


# generate_customer_data_select_query() # Run this first
# generate_address_update_transaction()