import psycopg2
import os
import logging
import requests
import json
import redis
from requests.exceptions import HTTPError
from dotenv import load_dotenv
load_dotenv()

logFormatter = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(format=logFormatter, level=logging.INFO)
logger = logging.getLogger(__name__)

conn = redis.Redis('redis')

debug = False


def get_interaction_data():
    try:
        connection = psycopg2.connect(user=os.getenv("user"),
            password=os.getenv("password"),
            host=os.getenv("host"),
            port=os.getenv("port"),
            database=os.getenv("database"))

        logger.info("Succesfully Connected to Database")
        cursor = connection.cursor()
        # we assume that the interactions are stored in a database
        postgreSQL_select_Query = "select * from --"
        cursor.execute(postgreSQL_select_Query)
        mobile_records = cursor.fetchall()

        if(len(mobile_records) == 0):
            logger.warning("No Database Records!")

        logger.debug("Database results length: " + str(len(mobile_records)))

        user_inter_dict = {}
        org_id = []
        asset_id = []
        score = []

        for row in mobile_records:
            org_id.append(row[0])
            asset_id.append(row[1])
            score.append(row[2])

        user_inter_dict["org_id"] = org_id
        user_inter_dict["asset_id"] = asset_id
        user_inter_dict["score"] = score

        logger.debug(user_inter_dict)

        conn.set("user-interaction", json.dumps(user_inter_dict))

        with open('/code/src/models_training/interaction_size', 'w') as f:
            f.write(str(len(user_inter_dict["org_id"])))

        connection.close()

        return user_inter_dict

    except (Exception, psycopg2.Error) as error:
        logger.error(f'Error while fetching data from Database: {error}')


def get_data_assets():
    try:
        logger.info("Trying to access data assets")
        data_assets = requests.get("http://" + os.getenv("icarus_internal")).json()

        if ('status' in data_assets) and ('error' in data_assets):
            if data_assets['status'] != 200:
                logger.error(
                    f'Error on data assets retrieval: {data_assets["error"]}')
                return None

        data_asset_list = []
        data_asset_dict = {}
        for data in data_assets:
            data_asset_list.append(data["id"])

        data_asset_dict["data-asset-list"] = data_asset_list

        conn.set("data-assets", json.dumps(data_asset_dict))

        return data_asset_list
    except HTTPError as http_err:
        logger.error(f'HTTP error occurred: {http_err}')
        return None
    except Exception as err:
        logger.error(f'Other error occurred: {err}')
        return None


def get_organizations():
    try:
        logger.info("Trying to access organizations")
        organizations = requests.get("http://" + os.getenv("icarus_internal")).json()

        if ('status' in organizations) and ('error' in organizations):
            if organizations['status'] != 200:
                logger.error(
                    f'Error on organizations retrieval: {organizations["error"]}')
                return None

        organziations_list = []
        organziations_dict = {}
        for org in organizations:
            organziations_list.append(org["id"])

        organziations_dict["organization-list"] = organziations_list

        conn.set("organizations", json.dumps(organziations_dict))

        return organziations_list
    except HTTPError as http_err:
        logger.error(f'HTTP error occurred: {http_err}')
        return None
    except Exception as err:
        logger.error(f'Other error occurred: {err}')
        return None


#
get_interaction_data()
get_data_assets()
get_organizations()
