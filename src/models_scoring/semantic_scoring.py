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
logging.basicConfig(format=logFormatter, level=logging.DEBUG)
logger = logging.getLogger(__name__)

conn = redis.Redis('redis')

debug = False


def get_data_model():
    """
    Retrieves the Aviation Data Model and returns a simplest dictionary having the sub-categories as keys and abstract categories as values.

    return: a dictionary with ADM's sub-categories as keys and abstract categories as values
    """
    logging.debug("Trying to retrieve data-model")

    path = "http://" + os.getenv("icarus_internal")

    try:
        data = requests.get(path).json()
        if 'status' in data:
            if data['status'] != 200:
                logger.error(
                    f'Error on data assets retrieval: {data["status"]}')
                return None
    except HTTPError as http_err:
        logger.error(f'HTTP error occurred: {http_err}')
        return None
    except Exception as err:
        logger.error(f'Other error occurred: {err}')
        return None

    data_model = []
    for cat in data:
        if 'text' in cat:
            data_model.append(cat['text'])
        if 'children' in cat:
            for sub_cat in cat['children']:
                data_model.append(sub_cat['text'])

    logging.debug("Data-Model successfully retrieved!")
    return data_model


###########################################################
def get_user_preferences(org_id, data_model):
    """
    This function retrieves and returns the preferences of the given user regarding the data categories that his organization is interested in (based on what the user mannually set in his profile).

    param org_id: a string containing the target user's organization ID
    param data_model: a dictionary with ADM's sub-categories as keys and abstract categories as values

    return: a list of strings containing the user's preferences
    """
    logging.debug("Trying to retrieve user preferences")

    prefix = "http://" + os.getenv("icarus_internal")

    path = prefix + str(org_id)

    try:
        data = requests.get(path).json()
        if 'status' in data:
            if data['status'] != 200:
                return []
    except HTTPError as http_err:
        logger.error(f'HTTP error occurred: {http_err}')
        return []
    except Exception as err:
        logger.error(f'Other error occurred: {err}')
        return []
    # checking for bad responses
    if 'categories' not in data:
        logger.warning(f"Categories are not in data!")
        logger.debug(data)
        return []

    # if everything is correct
    categories_set = set()
    for i in range(len(data['categories'])):
        cat = data['categories'][i]['name'].lower()
        if cat in data_model:
            categories_set.add(cat)
    user_preferences = list(categories_set)

    logger.debug("User preferences successfully retrieved!")
    logger.debug(f'User preferences: {user_preferences}')

    return user_preferences


###########################################################
def get_dataset_metadata(data_model):
    """
    This function retrieves and returns the category of each column of all datasets that are mapped to the Aviation Data Model.

    param data_model: a dictionary with ADM's sub-categories as keys and abstract categories as values

    return: two dictionaries (1) a dictionary with the data asset IDs as keys and for each one, a list of data categories per column as values; (2) a dictionary with extra information related to data assets (data asset id and name, owner id and name, cover photo)
    """
    logger.debug("Trying to retrieve dataset metadata")
    try:
        data = requests.get("http://" + os.getenv("icarus_internal")).json()

        if ('status' in data) and ('error' in data):
            if data['status'] != 200:
                return {}
    except HTTPError as http_err:
        logger.error(f'HTTP error occurred: {http_err}')
        return None
    except Exception as err:
        logger.error(f'Other error occurred: {err}')
        return None

    metadata = {}
    datasets_info = {}
    for i in range(len(data)):
        if 'id' not in data[i]:
            logger.warning("Id not in data[i]")
            continue
        dataset_id = data[i]['id']  # for data asset ID
        categories_list = []
        # for overall categories
        if 'categories' in data[i]:
            for j in range(len(data[i]['categories'])):
                if 'name' in data[i]['categories'][j]:
                    sub_cat = data[i]['categories'][j]['name'].lower()
                    if sub_cat in data_model:
                        categ = sub_cat
                        categories_list.append(categ)
        # for column sub-categories
        if 'columns' in data[i]:
            for j in range(len(data[i]['columns'])):
                if 'title' in data[i]['columns'][j]:
                    sub_categories = data[i]['columns'][j]['title'].split(".")
                    for sub_cat in sub_categories:
                        sub_cat = sub_cat.lower()
                        if sub_cat in data_model:
                            categ = sub_cat
                            categories_list.append(categ)
        metadata[dataset_id] = categories_list[:]
        # extra information related to data assets
        dataset_name = ""
        org_id = ""
        org_name = ""
        coverphoto = ""
        if 'name' in data[i]:
            dataset_name = data[i]['name']
        if 'coverphoto' in data[i]:
            coverphoto = data[i]['coverphoto']
        if 'organization' in data[i]:
            if 'id' in data[i]['organization']:
                org_id = data[i]['organization']['id']
            if 'name' in data[i]['organization']:
                org_name = data[i]['organization']['name']
        datasets_info[dataset_id] = {"dataset_id": dataset_id, "dataset_name": dataset_name, "org_id": org_id,
                                     "org_name": org_name, "coverphoto": coverphoto}

    try:
        if(debug):
            with open('./src/models_scoring/datasets_info', 'w') as f:
                json.dump(datasets_info, f)
        else:
            with open('/code/src/models_scoring/datasets_info', 'w') as f:
                json.dump(datasets_info, f)
    except OSError as err:
        logger.error('While writing the datasets information: {0}'.format(err))

    logger.debug("Dataset metadata successfully retrieved!")

    return metadata  # , datasets_info

###########################################################


def semantic_model(user_preferences, dataset_metadata, data_model):
    """
    This function implements the Semantic Content-based Recommendation Model.

    Given the user preferences and the entities mapped to the Aviation Data Model for each column of each dataset, the model computes a score (dot product) for each dataset.

    param user_preferences: a list of strings containing the user's preferences
    param dataset_metadata: a dictionary with the data asset IDs as keys and for each one, a list of data categories per column as values
    param data_model: a dictionary with ADM's sub-categories as keys and abstract categories as values

    return: a 2-D sorted list, with the inner list containing the data asset IDs and their scores based on the model
    """
    logger.debug("Trying to calculate semantic model")
    scores = []
    for dataset_id in dataset_metadata:
        ssum = 0
        count = 0
        for up in user_preferences:
            for col in dataset_metadata[dataset_id]:
                if up == col:
                    ssum += 1
                count += 1
        count = count * len(user_preferences)
        if count > 0:
            score = float(ssum) / count
        else:
            score = 0.

        scores.append([dataset_id, score])
    scores = sorted(scores, key=lambda l: l[1], reverse=True)
    logger.debug("Semantic model successfully calculated")

    return scores


def calculate_semantic_for_org(org_id):
    data_model = get_data_model()
    user_preferences = get_user_preferences(org_id, data_model)
    dataset_metadata = get_dataset_metadata(data_model)
    scores = semantic_model(user_preferences, dataset_metadata, data_model)
    score_list = []  # normalize semantic score 1-3
    for score in scores:
        score_list.append(score[1])
    logger.debug(f'Semantic Score List: {score_list}')
    for score in scores:
        try:
            score[1] = (score[1] - min(score_list)) / (
                max(score_list) - min(score_list)) * 2 + 1  # normilize between 1-3
        except ZeroDivisionError:
            logger.warning("Division by zero!")
            score[1] = 0

    sorted_by_semantic = sorted(scores, key=lambda l: l[0])
    return sorted_by_semantic


def semantic_scoring():
    organizations = json.loads(conn.get('organizations'))['organization-list']
    if (len(organizations) == 0):
        logger.warning("No organizations retrieved")
        return None

    scoring_dict = {}
    logger.info("Calculating Semantic score for each organization")
    for org in organizations:
        scores = calculate_semantic_for_org(org)
        scoring_dict[org] = scores

    logger.info("Semantic scoring matrix successfully created!")

    try:
        if(debug):
            with open('./src/models_scoring/semantic_scoring', 'w') as f:
                json.dump(scoring_dict, f)
        else:
            with open('/code/src/models_scoring/semantic_scoring', 'w') as f:
                json.dump(scoring_dict, f)
    except OSError as err:
        logger.error(
            'While writing the semantic score matrix: {0}'.format(err))
    logger.debug(f'Matrix length: {len(scoring_dict)} organizations')


semantic_scoring()
