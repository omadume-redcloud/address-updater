Install python3 if haven't got it already (can check using `python3 --version`)  
Create a virtual env: `python3 -m venv venv`  
Activate virtual env: `source venv/bin/activate` (don't need `source` if using Windows)  
Run the program (using an interactive session): `python -i main.py`  
Generate SQL statement for retrieving the customer details: `generate_customer_data_select_query()` (make sure to update file paths inside the method first)  
Run the outputted SELECT statement on the DB  
Export the results table as a CSV, and save it to this project root  
Generate the SQL transaction for updating the customer addresses: `generate_address_update_transaction()` (make sure to update file paths inside the method first)  
Run the outputted SQL transaction on the DB
Exit the interactive session: `exit`
Deactivate virtual env: `deactivate`  
