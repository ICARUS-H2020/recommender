import os
import json
import requests
from requests.exceptions import HTTPError
from surprise import dump
from dotenv import load_dotenv
import logging
import sys
import redis

load_dotenv()

logFormatter = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(format=logFormatter, level=logging.INFO)
logger = logging.getLogger(__name__)

conn = redis.Redis('redis')

debug = False

def get_semantic_scores(org_id):
    logger.debug("Trying to retrieve semantic scoring")

    try:
        if (debug):
            with open('./src/models_scoring/semantic_scoring') as f:
                semantic_scoring = json.load(f)
        else:
            with open('/code/src/models_scoring/semantic_scoring') as f:
                semantic_scoring = json.load(f)
    except OSError as err:
        logger.critical('While reading semantic scoring: {0}'.format(err))

    try:
        items = semantic_scoring[org_id]
    except:
        logger.warning(f'Organization {org_id} does not exists in semantic scoring matrix')
        items = semantic_scoring["1"]
    sorted_by_semantic = sorted(items, key=lambda l: l[0])

    logger.debug("Item based scoring successfully retrieved!")

    return sorted_by_semantic


def get_item_scores(org_id):
    logger.debug("Trying to retrieve item based scoring")

    try:
        if (debug):
            with open('./src/models_scoring/item_based_scoring') as f:
                item_scoring = json.load(f)
        else:
            with open('/code/src/models_scoring/item_based_scoring') as f:
                item_scoring = json.load(f)
    except OSError as err:
        logger.critical('While reading item based scoring: {0}'.format(err))

    try:
        items = item_scoring[org_id]
    except:
        logger.warning(f'Organization {org_id} does not exists in item scoring matrix')
        items = item_scoring["1"]
    sorted_by_items = sorted(items, key=lambda l: l[0])

    logger.debug("Item based scoring successfully retrieved!")

    return sorted_by_items


def get_user_scores(org_id):
    logger.debug("Trying to retrieve user based scoring")

    try:
        if (debug):
            with open('./src/models_scoring/user_based_scoring') as f:
                user_scoring = json.load(f)
        else:
            with open('/code/src/models_scoring/user_based_scoring') as f:
                user_scoring = json.load(f)
    except OSError as err:
        logger.critical('While reading user based scoring: {0}'.format(err))

    try:
        users = user_scoring[org_id]
    except:
        users = user_scoring["1"]
    sorted_by_users = sorted(users, key=lambda l: l[0])

    logger.debug("User based scoring successfully retrieved!")

    return sorted_by_users

def smallest(num1, num2, num3):
    if (num1 < num2) and (num1 < num3):
        smallest_num = num1
    elif (num2 < num1) and (num2 < num3):
        smallest_num = num2
    else:
        smallest_num = num3
    return smallest_num

def hybrid_model(semantic_scores, item_score, user_score):
    """
    This function implements the Semantic Content-based Recommendation Model.

    Given the user preferences and the entities mapped to the Aviation Data Model for each column of each dataset, the model computes a score (dot product) for each dataset.

    param semantic_scores: a sorted list of list, with the inner lists consist of the data asset IDs and their scores based on the model
    """
    logger.debug("Trying to calculate hybrid recommendation model")
    sparsity = 0
    try:
        if (debug):
            with open('./src/models_scoring/sparsity', 'r') as f:
                sparsity = float(f.readline())
        else:
            with open('/code/src/models_scoring/sparsity', 'r') as f:
                sparsity = float(f.readline())
    except OSError as err:
        logger.error('While reading sparsity: {0}'.format(err))

    hybrid_scores = []
    length = smallest(len(semantic_scores),len(item_score),len(user_score))
    for i in range(length):
        asset = [semantic_scores[i][0]]
        score = semantic_scores[i][1] * (1 - sparsity) + (((item_score[i][1] + user_score[i][1]) / 2) * sparsity)
        asset.append(score)
        hybrid_scores.append(asset)

    logger.debug("Hybrid Recommendation model successfully calculated!")

    return hybrid_scores


def generate_recommendation():

    logger.info("Trying to generate recommendations")
    organizations = json.loads(conn.get('organizations'))['organization-list']
    if (len(organizations) == 0):
        logger.warning("No organizations retrieved")
        return None

    logger.debug(organizations)
    recommendations_dict = {}
    for org in organizations:
        org = str(org)
        sorded_by_semantic = get_semantic_scores(org)
        sorted_by_items = get_item_scores(org)
        sorted_by_users = get_user_scores(org)
        recommendations = hybrid_model(sorded_by_semantic, sorted_by_items, sorted_by_users)
        recommendations_dict[org] = recommendations

    try:
        if(debug):
            with open('./src/recommendations', 'w') as f:
                json.dump(recommendations_dict, f)
        else:
            with open('/code/src/recommendations', 'w') as f:
                json.dump(recommendations_dict, f)
    except OSError as err:
        logger.error('While writing the recommendations matrix: {0}'.format(err))
    # print(scoring_dict)
    logger.debug(f'Matrix length: {len(recommendations_dict)} organizations')
    logger.info("Generate recommendations successfully calculated!")

generate_recommendation()
