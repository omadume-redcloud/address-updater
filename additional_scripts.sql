-- Backup scripts:
-- For entire tables
CREATE TABLE backup_customer_entity AS SELECT * FROM customer_entity;
CREATE TABLE backup_customer_address_entity AS SELECT * FROM customer_address_entity;
-- For the specific customer records only
CREATE TABLE backup_customer_entity AS SELECT * FROM customer_entity WHERE entity_id IN (<customer_ids>);
CREATE TABLE backup_customer_address_entity AS SELECT * FROM customer_address_entity WHERE entity_id IN (<customer_ids>);

-- Validation scripts (after transaction completed successfully):
SELECT 
    (CASE 
        WHEN (SELECT MD5(GROUP_CONCAT(default_billing, default_shipping ORDER BY entity_id)) 
              FROM customer_entity) != 
             (SELECT MD5(GROUP_CONCAT(default_billing, default_shipping ORDER BY entity_id)) 
              FROM backup_customer_entity) 
        THEN 'True' 
        ELSE 'False' 
     END) AS data_changed;

SELECT 
    (CASE 
        WHEN (SELECT COUNT(*) FROM customer_address_entity) > (SELECT COUNT(*) FROM backup_customer_address_entity) 
        THEN 'True' 
        ELSE 'False' 
     END) AS new_addresses_added;

-- Rollback scripts:
-- Reset default billing and shipping values to previous
UPDATE customer_entity
JOIN backup_customer_entity ON customer_entity.entity_id = backup_customer_entity.entity_id
SET customer_entity.default_billing = backup_customer_entity.default_billing,
    customer_entity.default_shipping = backup_customer_entity.default_shipping
WHERE customer_entity.entity_id IN (<customer_ids>);
-- Delete newly-added address records
SELECT GROUP_CONCAT(address_entity_id SEPARATOR ',') AS address_ids_to_export FROM <db_name>.new_address_ids; -- Then export as a csv and use for delete below!
DELETE FROM customer_address_entity WHERE entity_id IN (<new_address_record_ids>);
-- Validate rollbacks were successful
SELECT 
    (CASE 
        WHEN (SELECT MD5(GROUP_CONCAT(default_billing, default_shipping ORDER BY entity_id)) 
              FROM customer_entity) = 
             (SELECT MD5(GROUP_CONCAT(default_billing, default_shipping ORDER BY entity_id)) 
              FROM backup_customer_entity) 
        THEN 'True' 
        ELSE 'False' 
     END) AS checksums_match;

SELECT 
    (CASE 
        WHEN (SELECT COUNT(*) FROM customer_address_entity) = (SELECT COUNT(*) FROM backup_customer_address_entity) 
        THEN 'True' 
        ELSE 'False' 
     END) AS counts_match;
-- Cleanup new_address_ids which is no longer needed
DROP TABLE <db_name>.new_address_ids;