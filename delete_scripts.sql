-- Backup script & validation:
CREATE TABLE <db_name>.backup_customer_address_entity AS SELECT * FROM <db_name>.customer_address_entity;
-- Validate backup table is matching before executing implementation script
SELECT 
    (CASE 
        WHEN (SELECT COUNT(*) FROM <db_name>.customer_address_entity) = (SELECT COUNT(*) FROM <db_name>.backup_customer_address_entity) 
        THEN 'True' 
        ELSE 'False' 
     END) AS counts_match;

-- Implementation script & validation:
-- Delete all address records which are set as the default billing one for these customers
SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;

START TRANSACTION;

DELETE FROM <db_name>.customer_address_entity cae
WHERE cae.parent_id IN (<customer_ids>)
  AND cae.entity_id NOT IN (
    SELECT default_billing
    FROM <db_name>.customer_entity ce
    WHERE ce.entity_id IN (<customer_ids>)
  );

-- If no errors, uncomment and execute the COMMIT line below. Else, uncomment and execute ROLLBACK!
-- COMMIT;
-- ROLLBACK;

-- Validate table has been updated correctly (after implementation script has completed successfully)
SELECT 
    CASE 
        WHEN NOT EXISTS (
            SELECT 1 
            FROM <db_name>.customer_address_entity
            WHERE parent_id IN (customer_ids>)
            GROUP BY parent_id
            HAVING COUNT(*) != 1
        ) THEN 'True'
        ELSE 'False'
    END AS all_have_one_address;

-- Rollback script:
-- Restore deleted address records from backup table
SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE; -- Fully locking tables from inserts, etc as disabling and resetting auto-increment as part of transaction!

START TRANSACTION;

-- Temporarily disable auto-increment for entity_id
ALTER TABLE <db_name>.customer_address_entity AUTO_INCREMENT = 0;

INSERT INTO <db_name>.customer_address_entity (entity_id, increment_id, parent_id, created_at, updated_at, is_active, city, company, country_id, fax, firstname, lastname, middlename, postcode, prefix, region, region_id, street, suffix, telephone, vat_id, vat_is_valid, vat_request_date, vat_request_id, vat_request_success, delivery_instructions)
SELECT entity_id, increment_id, parent_id, created_at, updated_at, is_active, city, company, country_id, fax, firstname, lastname, middlename, postcode, prefix, region, region_id, street, suffix, telephone, vat_id, vat_is_valid, vat_request_date, vat_request_id, vat_request_success, delivery_instructions
FROM <db_name>.backup_customer_address_entity bcae
WHERE bcae.parent_id IN (<customer_ids>)
  AND bcae.entity_id NOT IN (
    SELECT default_billing
    FROM <db_name>.customer_entity ce
    WHERE ce.entity_id IN (<customer_ids>)
  );

-- Retrieve the next auto-increment value, and re-enable auto-increment for entity_id using it
SELECT MAX(entity_id) + 1 AS next_increment_value FROM <db_name>.customer_address_entity; -- do first and input result in line below!
ALTER TABLE <db_name>.customer_address_entity AUTO_INCREMENT = <next_increment_value>;

-- If no errors, uncomment and execute the COMMIT line below. Else, uncomment and execute ROLLBACK!
-- COMMIT;
-- ROLLBACK;

-- Resetting DB session to a more regular isolation level (after commiting transaction above)!
SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;

-- Validate rollback was successful
SELECT 
    CASE 
        WHEN NOT EXISTS (
            SELECT 1 
            FROM <db_name>.customer_address_entity cae
            LEFT JOIN <db_name>.backup_customer_address_entity bcae
                ON cae.parent_id = bcae.parent_id
            WHERE cae.parent_id IN (<customer_ids>)
            GROUP BY cae.parent_id
            HAVING COUNT(cae.entity_id) != COUNT(bcae.entity_id)
        ) THEN 'True'
        ELSE 'False'
    END AS addresses_restored;


-- Cleanup script:
DROP TABLE <db_name>.backup_customer_address_entity;
