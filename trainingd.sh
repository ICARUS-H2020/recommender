#!/bin/bash

PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/local/games:/usr/games
SHELL=/bin/bash

echo "Training.sh Started running"
python ./src/models_scoring/semantic_scoring.py
python ./src/models_training/item_based_model.py && python ./src/models_scoring/item_based_scoring.py
python ./src/models_training/user_based_model.py && python ./src/models_scoring/user_based_scoring.py
python ./src/hybrid_recommendations.py
