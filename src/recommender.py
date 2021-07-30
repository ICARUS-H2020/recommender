import os
import json
import requests
import sys
import logging
from requests.exceptions import HTTPError
from surprise import dump
from flask import request
from flask import jsonify
from flask_api import FlaskAPI
from flask_api import exceptions
from flask import redirect, url_for
from dotenv import load_dotenv
load_dotenv()

logFormatter = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(format=logFormatter, level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Create the application instance
app = FlaskAPI(__name__)
###########################################################

debug = False
debugauth = False  # FALSE ON PRODUCTION


def test():
    print(test)


def get_recommendations(org_id):

    logger.debug("Trying to retrieve recommendations")

    try:
        if(debug):
            with open('./src/recommendations') as f:
                recommendation = json.load(f)
        else:
            with open('/code/src/recommendations') as f:
                recommendation = json.load(f)
    except OSError as err:
        logger.critical('While reading recommendations: {0}'.format(err))

    try:
        recommendation_user = recommendation[org_id]
    except:
        recommendation_user = recommendation["1"]

    logger.debug("Recommendations successfully retrieved!")

    return recommendation_user


def get_dataset_info(recommended_dataset_ids):

    logger.debug("Trying to retrieve datasets information")

    try:
        if(debug):
            with open('./src/models_scoring/datasets_info') as f:
                dataset_info = json.load(f)
        else:
            with open('/code/src/models_scoring/datasets_info') as f:
                dataset_info = json.load(f)
    except OSError as err:
        logger.critical('While reading datasets information: {0}'.format(err))

    recommended_datasets = []
    for dataset_id in recommended_dataset_ids:
        try:
            dataset = dataset_info[str(dataset_id)]
            recommended_datasets.append(dataset)
        except:
            logger.warning(
                f"Dataset's ({dataset_id}) information cannot be retrieved")
            continue

    logger.debug("Dataset's information successfully retrieved!")

    return recommended_datasets


###########################################################
def datasets_not_owned(org_id, dataset_ids):
    """
    This function is responsible to retrieve the IDs of all data assets that a given organization owns or purchased and exclude them from the given recommended list.

    param org_id: a string containing the target user's organization ID
    param datasets_id: a list of data assets IDs

    return: a list of a list of data assets IDs
    """
    logger.debug("Trying to exclude owned datasets")

    prefix = "http://" + os.getenv("icarus_internal")
    path = prefix + str(org_id)
    try:
        data = requests.get(path).json()
        if 'status' in data:
            if data['status'] != 200:
                return dataset_ids
    except HTTPError as http_err:
        logger.error(f'HTTP error occurred: {http_err}')
        return []
    except Exception as err:
        logger.error(f'Other error occurred: {err}')
        return []
    # checking for bad responses
    owned = set()
    for i in range(len(data)):
        if 'id' not in data[i]:
            continue
        owned.add(data[i]['id'])
    dataset_ids = [x for x in dataset_ids if x not in list(owned)]

    logger.debug('Owned Datasets successfully excluded')
    logger.debug(f'List: {dataset_ids}')

    return dataset_ids

###########################################################


def datasets_not_visible(org_id, dataset_ids):
    """
    This function is responsible to retrieve the IDs of all data assets that a given organization owns or purchased and exclude them from the given recommended list.

    param org_id: a string containing the target user's organization ID
    param datasets_id: a list of data assets IDs

    return: a list of a list of data assets IDs
    """
    logger.debug("Trying to exclude owned datasets")

    prefix = "http://" + os.getenv("icarus_internal")
    path = prefix + str(org_id)
    try:
        data = requests.get(path).json()
        if 'status' in data:
            if data['status'] != 200:
                return dataset_ids
    except HTTPError as http_err:
        logger.error(f'HTTP error occurred: {http_err}')
        return []
    except Exception as err:
        logger.error(f'Other error occurred: {err}')
        return []
    # checking for bad responses
    logger.debug(f'Not visible data assets for org {org_id}: {data}')

    dataset_ids = [x for x in dataset_ids if x not in list(data)]

    logger.debug('Not visible Datasets successfully excluded')
    logger.debug(f'List: {dataset_ids}')

    return dataset_ids


def remove_deleted_datasets(dataset_ids):
    """
    This function is responsible to retrieve the IDs of all data assets that a given organization owns or purchased and exclude them from the given recommended list.

    param org_id: a string containing the target user's organization ID
    param datasets_id: a list of data assets IDs

    return: a list of a list of data assets IDs
    """
    logger.debug("Trying to exclude deleted datasets")

    prefix = "http://" + os.getenv("icarus_internal")
    path = prefix
    try:
        data = requests.get(path).json()
        if 'status' in data:
            if data['status'] != 200:
                return dataset_ids
    except HTTPError as http_err:
        logger.error(f'HTTP error occurred: {http_err}')
        return []
    except Exception as err:
        logger.error(f'Other error occurred: {err}')
        return []
    # checking for bad responses
    logger.debug(f'Not deleted data assets {data}')

    dataset_ids = [x for x in dataset_ids if x in list(data)]
    deleted_dataset_ids = [x for x in dataset_ids if x not in list(data)]

    logger.debug(f'Deleted data assets: {deleted_dataset_ids}')

    logger.debug('Deleted Datasets successfully excluded')
    logger.debug(f'List: {dataset_ids}')

    return dataset_ids

###########################################################


def generate_recommendations(org_id):
    """
    This function is responsible to retrieve all information needed from the storage and apply the recommendation models to generate the recommendations.

    param org_id: a string containing the target user's organization ID
    param datasets_id: a list of strings containing the data assets IDs

    return: a list of dictionaries, containing the recommended data assets along with other information in a descending order (most to less relevant)
    """
    logger.info("Generating Recommendations!")

    recommendations = get_recommendations(org_id)
    recommended_dataset_ids = [i[0] for i in recommendations]
    recommended_dataset_ids = datasets_not_owned(
        org_id, recommended_dataset_ids)
    recommended_dataset_ids = datasets_not_visible(
        org_id, recommended_dataset_ids)
    recommended_dataset_ids = remove_deleted_datasets(recommended_dataset_ids)
    recommended_datasets = get_dataset_info(recommended_dataset_ids)

    logger.info("Recommendations successfully generated!")

    return recommended_datasets[:10]


###########################################################
def generate_recommendations_for_dataset_id(dataset_id):
    try:
        if (debug):
            _, algo_item = dump.load(
                "./src/models_training/item_base_model_dump")
        else:
            _, algo_item = dump.load(
                "/code/src/models_training/item_base_model_dump")
    except OSError as err:
        logger.error('While reading the item based model: {0}'.format(err))
        return None

    dataset_id_str = str(dataset_id)
    item_inner_id = algo_item.trainset.to_inner_iid(dataset_id_str)
    neighbors = algo_item.get_neighbors(item_inner_id, k=10)
    logger.debug(f'Top 10 neighbors of dataset {dataset_id}: {neighbors}')

    return neighbors


###########################################################
def check_authentication(head):
    """
    Checks the authentication when Recommender is called.

    param headers: the headers of the POST request to the Recommender

    return: True if the authentication is valid; False otherwise
    """
    cookie = ""
    if 'Cookie' in head:
        cookie = head['Cookie']
    if cookie == "":
        return False
    URL = os.getenv("icarus_api")
    headers = {'Cookie': cookie}
    try:
        response = requests.get(url=URL, headers=headers)
        if response.status_code == 200:
            return True
    except HTTPError as http_err:
        logger.error(f'HTTP error occurred: {http_err}')
        return False
    except Exception as err:
        logger.error(f'Other error occurred: {err}')
        return False
    return False
###########################################################
# Create a URL route in our application for "/api/v1/recommender/"


@app.route('/api/v1/recommender/', methods=['POST'], strict_slashes=False)
def recommendation():
    """
    This function handles POST requests for the Recommender and returns a list of (recommended) data asset IDs.
    """
    # print(request.method)
    recommendations = []
    if request.method == 'POST':
        # check request body
        content = request.get_json()
        if content is None:
            # status "400 Bad Request"
            raise exceptions.ParseError(detail="Request body is empty.")
        # check request headers
        headers = request.headers
        if headers is None:
            # status "400 Bad Request"
            raise exceptions.ParseError(detail="Request headers are empty.")
        # check authentication cookie
        auth_flag = check_authentication(headers)
        if(debugauth):
            auth_flag = True
        if not auth_flag:
            # status "401 Unauthorized"
            return jsonify({"message": "ERROR: Unauthorized"}), 401
        if "org_id" in content:
            org_id = str(content['org_id'])
        else:
            # status "400 Bad Request"
            raise exceptions.ParseError(
                detail="Request body does not contain user ID.")
        if "datasets_id" in content:
            datasets_id = content['datasets_id']
        else:
            datasets_id = []
        recommendations = generate_recommendations(org_id)
    return jsonify({"recommended_datasets": recommendations})

if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=False)  # in production, debug=False

'''
Request Body
{
	"org_id": "1",
	"datasets_id": [
		"dataset1",
		"dataset2"
	]
}

Response Body
{
	"recommended_datasets": [
		{
			"dataset_id": "123",
			"dataset_name": "dataset name",
			"org_id": "123",
			"org_name": "owner organization name",
			"coverphoto": "11"
		},
		{
			...
		},
		{
			"dataset_id": "456",
			"dataset_name": "dataset name",
			"org_id": "456",
			"org_name": "owner organization name",
			"coverphoto": "22"
		}
	]
}
'''
