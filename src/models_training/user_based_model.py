import pandas as pd
import numpy as np
import sys
import logging
from surprise import KNNBaseline
from surprise import Dataset
from surprise import Reader
from surprise import dump
import redis
import json

logFormatter = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(format=logFormatter, level=logging.INFO)
logger = logging.getLogger(__name__)

conn = redis.Redis('redis')

debug = False


def data_extraction():
    user_inter_dict = json.loads(conn.get("user-interaction"))
    logger.debug(user_inter_dict)
    if (user_inter_dict is None or len(user_inter_dict["org_id"]) == 0):
        logger.warning("No available interaction data!")
        return None

    df = pd.DataFrame(user_inter_dict)
    reader = Reader(rating_scale=(1, 3))
    data = Dataset.load_from_df(df[['org_id', 'asset_id', 'score']], reader)

    return data


def model_training(data):
    sim_options = {'name': 'cosine',
                    'user_based': True  # compute  similarities between items
                    }
    algo_user = KNNBaseline(sim_options=sim_options)
    trainset = data.build_full_trainset()
    algo_user.fit(trainset)

    try:
        if (debug):
            dump.dump("./src/models_training/user_base_model_dump", algo= algo_user)
        else:
            dump.dump("/code/src/models_training/user_base_model_dump", algo= algo_user)
        logger.info("User-based model successfully dump to disk")
    except OSError as err:
        logger.error("While writing user-based model to file: {0}".format(err))


def main():
    model_data = data_extraction()
    if(model_data is None):
        return None
    model_training(model_data)


main()