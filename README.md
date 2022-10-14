# Backup Application Databases

Python script to take Backup of Databases, store it in DigitalOcean Spaces and report the status to Telegram Channel.

## Requirements

- Python 3.8.10 and external Python Modules:
    - MySQL Connector: `pip install mysql-connector-python`
    - Boto3: `pip install boto3`

## Config Data

**Database:**

- `DB_USERNAME` - Username to connect Database
- `DB_PASSWORD` - Password to connect of Database
- `DB_DATABASE` - Single Database or Comma separated Databases list

**DigitalOcean API**

- `DO_SPACES_KEY` - Spaces access Key
- `DO_SPACES_SECRET` - Secret generated along with the Spaces access Key
- `DO_SPACES_REGION` - Spaces region
- `DO_SPACES_BUCKET` - Bucket name in the Spaces Instance
- `DO_SPACES_ENDPOINT` - API endpoint
- `DO_SPACES_ROOT_FOLDER` - (Optional) Base folder to upload the files like name of the Application incase, single Spaces instance is used for multiple applications

**Telegram Bot** (To report status on Telegram Channel)

- `TG_BOT_TOKEN` - Access token of the Telegram bot
- `TG_BOT_CHANNEL_ID` - Chat ID in which the Bot has to report the status. Should be obtained via **getUpdates** method to Telegram Bot API (https://core.telegram.org/bots/api#getupdates) after adding the Bot to a Channel or after Sending 'Hi' to the bot incase of Personal chat
