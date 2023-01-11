from configparser import ConfigParser

# Settings variables
config = ConfigParser()
config.read('config.ini')

#[Confidence-Interval]
CI_MULTIPLIER_FACTOR_HIGH= int(config.get('Confidence-Interval', 'CI_MULTIPLIER_FACTOR_HIGH'))
CI_MULTIPLIER_FACTOR_MID= int(config.get('Confidence-Interval', 'CI_MULTIPLIER_FACTOR_MID'))

# [Database-Credentials]
DB_PORT= str(config.get('Database-Credentials', 'DB_PORT'))
DB_USER = str(config.get('Database-Credentials', 'DB_USER'))
DB_PASSWORD = str(config.get('Database-Credentials', 'DB_PASSWORD'))
DB_NAME = str(config.get('Database-Credentials', 'DB_NAME'))
DB_HOST_PROD = str(config.get('Database-Credentials', 'DB_HOST_PROD'))
DB_HOST_QA = str(config.get('Database-Credentials', 'DB_HOST_QA'))

#[New-Relic-Credentials]
NR_API_ENDPOINT= str(config.get('New-Relic-Credentials', 'NR_API_ENDPOINT'))
NR_API_KEY= str(config.get('New-Relic-Credentials', 'NR_API_KEY'))
