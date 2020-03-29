
# Bucket in GCS where all data will be synced
# TODO: move to configuration
BUCKET = 'gke-legion-dev01-data-store'
GS_FEEDBACK_PATH = 'gs://gke-legion-dev01-data-store/model_log'
API_HOST = 'odahu.gke-legion-dev01.ailifecycle.org'

TOPICS_COL = 'topics'
TEXT_COL = 'text'

TOPICS_PICKLE_FILE = 'topics.pickle'
DATASET_PICKLE_FILE = 'docs_df.pickle'
FEEDBACK_DATASET_PICKLE_FILE = 'feedback_docs_df.pickle'
COMBINED_DATASET_PICKLE_FILE = 'combined_docs_df.pickle'

TRAINING_INPUT_DIR = 'input/reuters-training'

# Inference
REALDATA_PICKLE = 'df.pickle'
INFERENCE_RESULT = 'result.json'
INFERENCE_HEADERS = 'headers.json'
REAL_RESULT = 'real_result.json'

# feedback
PREPARED_FEEDBACK = 'feedback.json'


