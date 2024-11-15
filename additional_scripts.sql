-- Backup scripts:
-- For entire tables (if changing to do specific customers only, will need to update validation scripts below with WHERE condition also)
CREATE TABLE <db_name>.backup_customer_entity AS SELECT * FROM <db_name>.customer_entity;
CREATE TABLE <db_name>.backup_customer_address_entity AS SELECT * FROM <db_name>.customer_address_entity;

-- Validation scripts:
-- Validate backup tables are matching before executing transaction
SELECT 
    (CASE 
        WHEN (SELECT MD5(GROUP_CONCAT(default_billing, default_shipping ORDER BY entity_id)) 
              FROM <db_name>.customer_entity) = 
             (SELECT MD5(GROUP_CONCAT(default_billing, default_shipping ORDER BY entity_id)) 
              FROM <db_name>.backup_customer_entity) 
        THEN 'True' 
        ELSE 'False' 
     END) AS checksums_match;

SELECT 
    (CASE 
        WHEN (SELECT COUNT(*) FROM <db_name>.customer_address_entity) = (SELECT COUNT(*) FROM <db_name>.backup_customer_address_entity) 
        THEN 'True' 
        ELSE 'False' 
     END) AS counts_match;
-- Validate tables have been updated (after transaction has completed successfully)
SELECT 
    (CASE 
        WHEN (SELECT MD5(GROUP_CONCAT(default_billing, default_shipping ORDER BY entity_id)) 
              FROM <db_name>.customer_entity) != 
             (SELECT MD5(GROUP_CONCAT(default_billing, default_shipping ORDER BY entity_id)) 
              FROM <db_name>.backup_customer_entity) 
        THEN 'True' 
        ELSE 'False' 
     END) AS data_changed;

SELECT 
    (CASE 
        WHEN (SELECT COUNT(*) FROM <db_name>.customer_address_entity) > (SELECT COUNT(*) FROM <db_name>.backup_customer_address_entity) 
        THEN 'True' 
        ELSE 'False' 
     END) AS new_addresses_added;

-- Rollback scripts:
-- Reset default billing and shipping values to previous
UPDATE <db_name>.customer_entity
JOIN <db_name>.backup_customer_entity ON <db_name>.customer_entity.entity_id = <db_name>.backup_customer_entity.entity_id
SET <db_name>.customer_entity.default_billing = <db_name>.backup_customer_entity.default_billing,
    <db_name>.customer_entity.default_shipping = <db_name>.backup_customer_entity.default_shipping
WHERE <db_name>.customer_entity.entity_id IN (<customer_ids>);
-- Delete newly-added address records
SELECT GROUP_CONCAT(address_entity_id SEPARATOR ',') AS address_ids_to_export FROM <db_name>.new_address_ids; -- Then export as a csv and use for delete below!
DELETE FROM <db_name>.customer_address_entity WHERE entity_id IN (<new_address_record_ids>);
-- Validate rollbacks were successful
SELECT 
    (CASE 
        WHEN (SELECT MD5(GROUP_CONCAT(default_billing, default_shipping ORDER BY entity_id)) 
              FROM <db_name>.customer_entity) = 
             (SELECT MD5(GROUP_CONCAT(default_billing, default_shipping ORDER BY entity_id)) 
              FROM <db_name>.backup_customer_entity) 
        THEN 'True' 
        ELSE 'False' 
     END) AS checksums_match;

SELECT 
    (CASE 
        WHEN (SELECT COUNT(*) FROM <db_name>.customer_address_entity) = (SELECT COUNT(*) FROM <db_name>.backup_customer_address_entity) 
        THEN 'True' 
        ELSE 'False' 
     END) AS counts_match;

-- Cleanup scripts:
DROP TABLE <db_name>.new_address_ids;
DROP TABLE <db_name>.backup_customer_entity;
DROP TABLE <db_name>.backup_customer_address_entity;