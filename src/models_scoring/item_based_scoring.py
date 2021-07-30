import os
import json
import requests
from requests.exceptions import HTTPError
from surprise import dump
from dotenv import load_dotenv
import logging
import redis
import sys
load_dotenv()

logFormatter = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(format=logFormatter, level=logging.INFO)
logger = logging.getLogger(__name__)

conn = redis.Redis('redis')

debug = False

def calculate_sparsity(item_size,user_size):

    logger.info("Calculating Sparsity")
    try:
        if (debug):
            with open('./src/models_training/interaction_size', "r") as f:
                inter_size = int(f.readline())
        else:
            with open('/code/src/models_training/interaction_size', "r") as f:
                inter_size = int(f.readline())
        sparsity = inter_size / (item_size * user_size)
    except OSError as err:
        logger.error('While reading the interaction size: {0}'.format(err))
        sparsity = 0

    try:
        if(debug):
            with open('./src/models_scoring/sparsity', 'w') as f1:
                f1.write(str(sparsity))
        else:
            with open('/code/src/models_scoring/sparsity', 'w') as f1:
                f1.write(str(sparsity))
        logger.info("Sparsity successfully created!")
    except OSError as err:
        logger.error('While writing the sparsity: {0}'.format(err))


def model_prediction():
    try:
        if(debug):
            _, algo_item = dump.load("./src/models_training/item_base_model_dump")
        else:
            _, algo_item = dump.load("/code/src/models_training/item_base_model_dump")
    except OSError as err:
        logger.error('While reading the item based model: {0}'.format(err))
        return None

    data_assets = json.loads(conn.get('data-assets'))['data-asset-list']
    organizations = json.loads(conn.get('organizations'))['organization-list']

    if (len(data_assets)==0 or len(organizations)==0):
        logger.warning("No data assets or organizations retrieved")
        return None

    calculate_sparsity(len(data_assets), len(organizations))
    scoring_dict = {}
    logger.info("Calculating User-Item scoring matrix (item)")
    for org in organizations:
        score_list = []
        for dataset in data_assets:
            prediction = algo_item.predict(org, dataset)
            tmp_obj = []
            tmp_obj.append(dataset)
            tmp_obj.append(prediction[3])
            score_list.append(tmp_obj)

        scoring_dict[org] = score_list

    logger.info("User-Item scoring matrix successfully created! (item)")

    try:
        if (debug):
            with open('./src/models_scoring/item_based_scoring', 'w') as f:
                json.dump(scoring_dict, f)
        else:
            with open('/code/src/models_scoring/item_based_scoring', 'w') as f:
                json.dump(scoring_dict, f)
    except OSError as err:
        logger.error('While writing the user based score matrix: {0}'.format(err))
    # print(scoring_dict)
    logger.debug(f'Matrix length: {len(scoring_dict)} organizations (item)')

model_prediction()