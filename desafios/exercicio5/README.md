# Diagrama Exercicio 5 DataWarehouse

* ## Tabela Transacoes
A tabela ```Transacoes``` seria alimentada pelas principais tabelas (bankslip,pix_received, pix_send, p2p_tef) utilizadas na consulta anterior. Destas tabelas iriamos extrair as principais colunas para alimentar a tabela Transacoes, são elas: ```amount, account_id, timestamp, transaction_type```.

Conforme o diagrama, está tabela terá um relacionamento com a tabela ```Account``` através da coluna ```account_id```. E a tabela ```Account``` terá um relacionamento com a tabela ```Customer``` através da coluna ```customer_id```.

A vantagem de ter criado esta tabela ```Transacoes``` se dá pelo fato de termos todas as transações concentradas em uma unica tabela, com isso diminuindo as relações e custos da consulta anterior, sendo apenas executado a média dos valores e o agrupamento. E também temos a vantagem de poder utilizar esta tabela para outros tipos de consulta da área que envolva algum calculo com a coluna amount e com este tipo de relacionamento.

A desvantagem se dá pelo fato de uma tabela ficar com uma concentração grande de dados extraídos por outras tabelas. Este tipo de problema poderia ser resolvido caso essa tabela já fosse alimentada com os dados agrupados e calculados, porém com este tipo de solução não poderia ser reaproveitada a tabela, se tornando uma tabela existente do DW para apenas um tipo de problema.