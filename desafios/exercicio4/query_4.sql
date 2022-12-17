SELECT c.customer_id, a.account_id, c.name,
CAST(t.`timestamp` AS DATE) as date, t.transaction_type, AVG(t.amount) as 'mean_value' 
from customer c 
INNER JOIN account a ON a.customer_id = c.customer_id
INNER JOIN (
select amount, account_id_source as 'account_id', `timestamp`, 
'p2p_tef' as 'transaction_type'
from p2p_tef ppt 
UNION 
select amount, account_id, `timestamp`, 'bankslip' as 'transaction_type'
from bankslip b 
UNION 
select amount, account_id, `timestamp`, 'pix_received' as 'transaction_type'
from pix_received pr  
UNION
select amount, account_id, `timestamp`, 'pix_send' as 'transaction_type'
from pix_send ps  
) AS t ON t.account_id = a.account_id 
GROUP BY 
1,2,3,4,5
;