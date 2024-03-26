from Auxiliares_OKR_Pesados.passwords import DEALER_PASSWORD,POSTGRESS_PASSWORD

# Data inicial de inclusão dos regsitros nas querys
date_start = '2022-01-01'

# DW Corporativo
DEALER_USERNAME = 'bq_dwcorporativo_u'
DEALER_PASSWORD = DEALER_PASSWORD
DEALER_DBNAME = 'dbDealernetWF'
DEALER_HOST =  '10.13.72.11'
DEALER_PORT = '65002'


# DW Corporativo
DW_CORPORATIVO_USERNAME = 'master'
DW_CORPORATIVO_PASSWORD = POSTGRESS_PASSWORD
DW_CORPORATIVO_DBNAME = 'postgres'
DW_CORPORATIVO_HOST = 'db-dw-prod.postgres.database.azure.com'
DW_CORPORATIVO_PORT = '5432'
# Schema para a criação e inserção de registros do ETL
SCHEMA_CRIACAO_INSERT = 'staging'