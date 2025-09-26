import sys
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
print("Rodando Glue Job:", args['JOB_NAME'])