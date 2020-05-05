MERGE INTO customers
USING (
   -- These rows will either UPDATE the current addresses of existing customers or INSERT the new addresses of new customers
  SELECT updates.customerId as mergeKey, updates.*
  FROM updates
  
  UNION ALL
  
  -- These rows will INSERT new addresses of existing customers 
  -- Setting the mergeKey to NULL forces these rows to NOT MATCH and be INSERTed.
  SELECT NULL as mergeKey, updates.*
  FROM updates JOIN customers
  ON updates.customerid = customers.customerid 
  WHERE customers.current = true AND updates.address <> customers.address 
  
) staged_updates
ON customers.customerId = mergeKey
WHEN MATCHED AND customers.current = true AND customers.address <> staged_updates.address THEN  
  UPDATE SET current = false, endDate = staged_updates.effectiveDate    -- Set current to false and endDate to source's effective date.
WHEN NOT MATCHED THEN 
  INSERT(customerid, address, current, effectivedate, enddate) 
  VALUES(staged_updates.customerId, staged_updates.address, true, staged_updates.effectiveDate, null) -- Set current to true along with the new address and its effective date.
