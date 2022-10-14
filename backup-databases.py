import subprocess, os, traceback, logging, configparser, mysql.connector, zipfile, boto3, botocore, requests, json, argparse
from datetime import datetime
from typing import Final

# config file
CONFIG_FILE: Final = '.env'
# list of databases to exclude from backup
SYSTEM_DATABASES: Final = ['information_schema', 'mysql', 'performance_schema', 'sys']

# no of days to mark file as toDelete
MARK_TO_DELETE_NO_OF_DAYS: Final = 3

# telegram bot config
TG_BOT_TOKEN = ''
TG_BOT_CHANNEL_ID = ''

# initialize logging
logging.basicConfig(format = '[%(asctime)s] (%(levelname)s): %(message)s', datefmt = '%d-%m-%Y %H:%M:%S')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# __sendTelegramMessage
def __sendTelegramMessage(message, success):
    global logger, TG_BOT_TOKEN, TG_BOT_CHANNEL_ID

    if len(TG_BOT_TOKEN) > 0 and len(TG_BOT_CHANNEL_ID) > 0:
        logger.info('Sending message to Telegram channel')

        if not success:
            message = 'Backup failed:\n\n' + message

        response = requests.post(f'https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage', json = {
            'chat_id': TG_BOT_CHANNEL_ID,
            'text': message,
        })

        if response and response.ok and response.status_code != 204:
            jsonResponse = None

            logger.info('Parsing Telegram API response')

            try:
                jsonResponse = response.json()
            except Exception as e:
                logger.error('Error when parsing Telegram API response: ' + traceback.format_exc())
                quit()

            if jsonResponse and type(jsonResponse) is dict and 'ok' in jsonResponse and jsonResponse['ok']:
                logger.info('Telegram message sent.')

                return True
            else:
                logger.error('Invalid response from Telegram API: ' + json.dumps(jsonResponse))
                quit()

    return False
# __sendTelegramMessage

# __exit
def __exit(message, success = False):
    global logger

    if message and len(message) > 0:
        if success:
            logger.info(message)
        else:
            logger.error(message)

    __sendTelegramMessage(message, success)

    quit()
# __exit

# __initConfig
def __initConfig():
    global logger, CONFIG_FILE, TG_BOT_TOKEN, TG_BOT_CHANNEL_ID

    dbConfig = None
    doSpacesConfig = None

    if not os.path.isfile(CONFIG_FILE):
        __exit(f'{CONFIG_FILE} does not exists.')

    configFileContent = ''
    logger.info(f'Extracting config data from {CONFIG_FILE}')

    # read config file
    with open(CONFIG_FILE, 'r') as fpConfig:
        tempConfigContent = fpConfig.read().strip()

        if len(tempConfigContent) > 0:
            configFileContent = '[DEFAULT]\n' + tempConfigContent

    if len(configFileContent) == 0:
        __exit(f'{CONFIG_FILE} is empty.')

    # extract config data
    config = configparser.ConfigParser(allow_no_value = True)
    config.read_string(configFileContent)

    # check if telegram bot config data are valid
    if len(TG_BOT_TOKEN) == 0:
        if config.has_option('DEFAULT', 'TG_BOT_TOKEN'):
            TG_BOT_TOKEN = config.get('DEFAULT', 'TG_BOT_TOKEN')

            if len(TG_BOT_TOKEN) == 0:
                __exit(f'Invalid TG_BOT_TOKEN from {CONFIG_FILE}')
        else:
            __exit(f'TG_BOT_TOKEN not found in {CONFIG_FILE}')

    if len(TG_BOT_CHANNEL_ID) == 0:
        if config.has_option('DEFAULT', 'TG_BOT_CHANNEL_ID'):
            TG_BOT_CHANNEL_ID = config.get('DEFAULT', 'TG_BOT_CHANNEL_ID')

            if len(TG_BOT_CHANNEL_ID) == 0:
                __exit(f'Invalid TG_BOT_CHANNEL_ID from {CONFIG_FILE}')
        else:
            __exit(f'TG_BOT_CHANNEL_ID not found in {CONFIG_FILE}')

    # check if database config data are valid
    if config.has_option('DEFAULT', 'DB_USERNAME') and config.has_option('DEFAULT', 'DB_PASSWORD'):
        dbConfig = {
            'host': config.get('DEFAULT', 'DB_HOST') if config.has_option('DEFAULT', 'DB_HOST') else '127.0.0.1',
            'port': config.get('DEFAULT', 'DB_PORT') if config.has_option('DEFAULT', 'DB_PORT') else 3306,
            'user': config.get('DEFAULT', 'DB_USERNAME'),
            'password': config.get('DEFAULT', 'DB_PASSWORD'),
        }

        if config.has_option('DEFAULT', 'DB_DATABASE'):
            dbConfig['database'] = config.get('DEFAULT', 'DB_DATABASE')
        elif config.has_option('DEFAULT', 'DB_DATABASES'):
            dbConfig['database'] = config.get('DEFAULT', 'DB_DATABASES')
    else:
        __exit('Invalid Database config data! Please check if all the necessary details provided.')

    if not dbConfig:
        __exit(f'Unable to extract config data from {CONFIG_FILE}')

    # check if do spaces config data are valid
    if config.has_option('DEFAULT', 'DO_SPACES_KEY') and config.has_option('DEFAULT', 'DO_SPACES_SECRET') and config.has_option('DEFAULT', 'DO_SPACES_REGION') and config.has_option('DEFAULT', 'DO_SPACES_BUCKET') and config.has_option('DEFAULT', 'DO_SPACES_ENDPOINT'):
        doSpacesConfig = {
            'key': config.get('DEFAULT', 'DO_SPACES_KEY'),
            'secret': config.get('DEFAULT', 'DO_SPACES_SECRET'),
            'region': config.get('DEFAULT', 'DO_SPACES_REGION'),
            'bucket': config.get('DEFAULT', 'DO_SPACES_BUCKET'),
            'endpoint': config.get('DEFAULT', 'DO_SPACES_ENDPOINT'),
        }

        if config.has_option('DEFAULT', 'DO_SPACES_ROOT_FOLDER'):
            doSpacesConfig['rootFolder'] = config.get('DEFAULT', 'DO_SPACES_ROOT_FOLDER')

        for doSpacesConfigVar in doSpacesConfig:
            if len(doSpacesConfig[doSpacesConfigVar]) == 0:
                __exit(f'Invalid DO Space {doSpacesConfigVar} in {CONFIG_FILE}')
    else:
        __exit('Invalid DO Spaces config data! Please check if all the necessary details provided.')

    if not doSpacesConfig:
        __exit(f'Unable to extract DO Spaces config data from {CONFIG_FILE}')

    return dbConfig, doSpacesConfig
# __initConfig

# __getDatabasesListFromMySQL
def __getDatabasesListFromMySQL(dbConfig, backupAllDatabases = False, includeSystemDatabases = False):
    global logger, SYSTEM_DATABASES
    databasesToBackup = None

    if not backupAllDatabases:
        # get database list from config
        if 'database' in dbConfig:
            databasesToBackup = dbConfig['database'].split(',')

    mysqlConn = None
    logger.info('Checking MySQL credentials...')

    try:
        mysqlConn = mysql.connector.connect(
            host = dbConfig['host'],
            port = dbConfig['port'],
            user = dbConfig['user'],
            password = dbConfig['password'],
        )
    except Exception as e:
        __exit('Error occurred when trying to connect MySQL:' + traceback.format_exc())

    if not mysqlConn:
        __exit('Unable to connect to MySQL')

    # if take all backup including system, no need to verify databases, --databases argument will not be passed to mysqldump command
    if backupAllDatabases and includeSystemDatabases:
        return None

    logger.info('Fetching Databases list')

    dbCursor = mysqlConn.cursor(dictionary = True)
    dbCursor.execute("show databases")
    databasesList = [ row.get('Database') for row in dbCursor ]

    if databasesList and type(databasesList) is list and len(databasesList) > 0:
        # exclude system databases
        databasesList = [ tempDbName for tempDbName in databasesList if tempDbName not in SYSTEM_DATABASES ]

        if len(databasesList) == 0:
            __exit('There are no Databases to take Backup!')

        dbsNotFound = []

        if databasesToBackup:
            logger.info('Checking Databases\' existence')

            for dbName in databasesToBackup:
                if dbName not in databasesList:
                    dbsNotFound.append(dbName)
        else:
            if backupAllDatabases:
                logger.info('Selecting all databases to backup')
            else:
                logger.info('Databases list not provided, selecting all databases to backup')

            databasesToBackup = databasesList

        if len(dbsNotFound) > 0:
            __exit('Following databases are not found in MySQL: ' + (', '.join(dbsNotFound)))
    else:
        __exit('Unable to get List of databases from MySQL')

    return databasesToBackup
# __getDatabasesListFromMySQL

# __executeMySQLDumpCmd
def __executeMySQLDumpCmd(dbConfig, databasesToBackup):
    global logger

    logger.info('Running mysqldump command')

    today = datetime.now().strftime('%Y%m%d_%H%M%S')
    zipFileName = f'dbBackup-{today}.zip'
    mysqlDumpFileName = f'dbBackup-{today}.sql'
    mysqlDumpCmd = 'mysqldump -u ' + dbConfig['user'] + ' -p' + dbConfig['password']

    if databasesToBackup:
        mysqlDumpCmd += ' --databases ' + (' '.join(databasesToBackup))
    else:
        mysqlDumpCmd += ' --all-databases'

    mysqlDumpCmd += ' > ' + mysqlDumpFileName

    logger.debug('mysqldump command: ' + mysqlDumpCmd.replace('-p' + dbConfig['password'], '-p'))

    dumpCmdResult = subprocess.run(f'{mysqlDumpCmd}', shell = True, capture_output = True, text = True)

    if dumpCmdResult:
        if dumpCmdResult.returncode == 0:
            if os.path.isfile(mysqlDumpFileName):
                if os.path.getsize(mysqlDumpFileName) > 0:
                    pass
                else:
                    __exit(f'{mysqlDumpFileName} size is zero!')
            else:
                __exit(f'{mysqlDumpFileName} not created!')
        else:
            __exit('Error when running mysqldump command: ' + (dumpCmdResult.stderr or dumpCmdResult.stdout))
    else:
        __exit('Unable to run mysqldump command!')

    return mysqlDumpFileName, zipFileName
# __executeMySQLDumpCmd

# __compressBackupFile
def __compressBackupFile(mysqlDumpFileName, zipFileName):
    global logger

    logger.info('Compressing the backup file')

    with zipfile.ZipFile(zipFileName, 'w', compression = zipfile.ZIP_DEFLATED) as fpZip:
        fpZip.write(mysqlDumpFileName)

    if os.path.isfile(zipFileName):
        if os.path.getsize(zipFileName) > 0:
            os.remove(mysqlDumpFileName)
        else:
            __exit(f'{zipFileName} size is zero!')
    else:
        __exit(f'{zipFileName} not created!')
# __compressBackupFile

# __uploadBackupFileToDOSpace
def __uploadBackupFileToDOSpace(doSpacesConfig, zipFileName):
    global logger, MARK_TO_DELETE_NO_OF_DAYS

    s3Client = None
    logger.info('Establishing connection to DO Spaces')

    try:
        s3Session = boto3.session.Session()
        s3Client = s3Session.client(
            's3',
            config = botocore.config.Config(s3={'addressing_style': 'virtual'}),
            region_name = doSpacesConfig['region'],
            endpoint_url = doSpacesConfig['endpoint'],
            aws_access_key_id = doSpacesConfig['key'],
            aws_secret_access_key = doSpacesConfig['secret'],
        )
    except Exception as e:
        __exit('Error when establishing connection to DO Spaces:' + traceback.format_exc())

    logger.info(f'Checking if there is any backup file older than {MARK_TO_DELETE_NO_OF_DAYS} days')

    rootFolderPath = (doSpacesConfig['rootFolder'].rstrip('/') + '/' if len(doSpacesConfig['rootFolder']) > 0 else '')
    filesList = s3Client.list_objects(Bucket = doSpacesConfig['bucket'])

    if 'Contents' in filesList:
        toDeleteFolderPath = rootFolderPath + 'toDelete/'

        for fileObj in filesList['Contents']:
            if fileObj['LastModified'] and type(fileObj['LastModified']) is datetime:
                filePath = fileObj['Key']
                fileName = os.path.basename(filePath)

                # remove files marked toDelete
                if filePath.startswith(toDeleteFolderPath):
                    logger.info(f'Deleting {filePath}')

                    s3Client.delete_object(Bucket = doSpacesConfig['bucket'], Key = filePath)
                else:
                    thisDate = datetime.now().date()
                    fileLastModifiedDate = fileObj['LastModified'].date()

                    # check and mark files toDelete
                    if (thisDate - fileLastModifiedDate).days > MARK_TO_DELETE_NO_OF_DAYS:
                        logger.info(f'{fileName} is older than {MARK_TO_DELETE_NO_OF_DAYS} days. Moving it to "toDelete" folder')

                        s3Client.download_file(doSpacesConfig['bucket'], filePath, fileName)

                        with open(fileName, 'rb') as fpFileToMove:
                            s3Client.put_object(
                                Bucket = doSpacesConfig['bucket'],
                                Key = rootFolderPath + 'toDelete/' + fileName,
                                Body = fpFileToMove.read(),
                                ACL = 'private',
                            )

                        # remove moved file
                        s3Client.delete_object(Bucket = doSpacesConfig['bucket'], Key = filePath)
                        # remove downloaded local copy
                        os.remove(fileName)

    logger.info('Transferring the Backup file to DO Spaces')

    with open(zipFileName, 'rb') as fpZip:
        s3Client.put_object(
            Bucket = doSpacesConfig['bucket'],
            Key = rootFolderPath + zipFileName,
            Body = fpZip.read(),
            ACL = 'private',
        )

        os.remove(zipFileName)

    logger.info(f'{zipFileName} has been uploaded to DO Spaces')
# __uploadBackupFileToDOSpace

# main
def main():
    global logger

    try:
        dbConfig, doSpacesConfig = __initConfig()
        backupAllDatabases = False
        includeSystemDatabases = False

        cmdLineArgParser = argparse.ArgumentParser(description = 'Database backup: Take backup of application database(s) and upload to DigitalOcean Spaces.')
        cmdLineArgParser.add_argument('--allDatabases', help = 'Force to take backup of all the Databases.', action = 'store_true', default = False)
        cmdLineArgParser.add_argument('--includeSystemDatabases', help = 'Include system Databases to backup.', action = 'store_true', default = False)
        arguments = cmdLineArgParser.parse_args()

        if arguments:
            if 'allDatabases' in arguments:
                backupAllDatabases = arguments.allDatabases
            if 'includeSystemDatabases' in arguments:
                includeSystemDatabases = arguments.includeSystemDatabases

        databasesToBackup = __getDatabasesListFromMySQL(dbConfig, backupAllDatabases = backupAllDatabases, includeSystemDatabases = includeSystemDatabases)

        mysqlDumpFileName, zipFileName =__executeMySQLDumpCmd(dbConfig, databasesToBackup)

        __compressBackupFile(mysqlDumpFileName, zipFileName)

        __uploadBackupFileToDOSpace(doSpacesConfig, zipFileName)

        __exit(f'Backup has been successfully taken, file name: {zipFileName}', True)
    except Exception as e:
        __exit('Error occurred: ' + traceback.format_exc())
# main

if __name__ == '__main__':
    main()
