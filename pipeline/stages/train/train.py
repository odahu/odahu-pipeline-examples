
from __future__ import print_function

import argparse
import pickle
import os
from typing import List

import mlflow.keras
import mlflow.pyfunc
import pandas as pd
from keras import Sequential
from keras.layers import Dense, Activation, Dropout
from keras_preprocessing.text import Tokenizer
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MultiLabelBinarizer

from src.utils import ModelWrapper, save_samples

# Constants
REUTERS_DATA_PATH = 'data'
TOPICS_ENCODER_FILE = 'topics_encoder.pkl'
TOKENIZER_FILE = 'tokenizer.pkl'
KERAS_MODEL_FILE = 'keras_model.h5'
BATCH_SIZE = 32
EPOCHS = 5

# Parse parameters

arg_parser = argparse.ArgumentParser()
arg_parser.add_argument('--maxwords')
arg_parser.add_argument('--units')
args = arg_parser.parse_args()

max_words = int(args.maxwords or 1000)
units = int(args.units or 512)

print(f'Script is launched with params: max_words={max_words}, units={units}')

print('listdir')
print(list(os.listdir()))

with open('topics.pickle', 'rb') as f:
    topics: List[str] = pickle.load(f)
with open('docs_df.pickle', 'rb') as f:
    docs_df: pd.DataFrame = pickle.load(f)

# Tokenize all reuters dataset
tokenizer = Tokenizer(num_words=max_words)
tokenizer.fit_on_texts(docs_df['text'].to_list())

topics_encoder = MultiLabelBinarizer()
topics_encoder.fit(([t] for t in topics))


# Prepare features (encode and split)
X = tokenizer.texts_to_matrix(docs_df['text'].to_list())
Y = topics_encoder.transform(docs_df['topics'].to_list())
X_train, X_test, Y_train, Y_test = train_test_split(X, Y, test_size=0.2)


# Build model
print('Building model...')
num_classes = len(topics)
model = Sequential()
model.add(Dense(units, input_shape=(max_words,)))
model.add(Activation('relu'))
model.add(Dropout(0.5))
model.add(Dense(num_classes))
model.add(Activation('softmax'))

model.compile(loss='categorical_crossentropy',
              optimizer='adam',
              metrics=['accuracy'])


# Train model
with mlflow.start_run():

    mlflow.log_param("maxwords", max_words)
    mlflow.log_param("units", units)

    mlflow.keras.autolog()

    history = model.fit(X_train, Y_train,
                        batch_size=BATCH_SIZE,
                        epochs=EPOCHS,
                        verbose=1,
                        validation_split=0.1)
    score = model.evaluate(X_test, Y_test,
                           batch_size=BATCH_SIZE, verbose=1)

    model.save(KERAS_MODEL_FILE)

    artifacts = {
        'keras_model': KERAS_MODEL_FILE,
    }

    conda_env = 'conda.yaml'

    mlflow.pyfunc.log_model(
        'model', artifacts=artifacts, conda_env=conda_env,
        python_model=ModelWrapper(tokenizer, topics_encoder), code_path=['src']
    )

    # print some metrics
    print('Test score:', score[0])
    print('Test accuracy:', score[1])

    artifact_paths = save_samples(topics)
    for fp in artifact_paths:
        mlflow.log_artifact(fp, 'model')
