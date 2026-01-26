SELECT 
  -- If the name has a comma, we can assume it is in Last, First format. 
  -- Names should be in First Last format.
  name,
  donation_id,
  CASE WHEN CONTAINS(name, ',') THEN 1 ELSE 0 END AS dq_reversed_name
FROM
  data5035.spring26.donations;
