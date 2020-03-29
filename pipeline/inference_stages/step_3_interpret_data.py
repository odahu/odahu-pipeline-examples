import json
import logging
from typing import Dict, List

from airflow import DAG

from utils import const, config
from utils.workspace import inside_workspace


logger = logging.getLogger(__name__)


def interpret(request_result):

    topics_list: List[List[str]] = []

    predictions, columns = request_result['prediction'], request_result['columns']

    for pred in predictions:  # type: List[str]
        topics = []
        for i, topic_prob in enumerate(pred):
            if float(topic_prob.replace('%', '')) > 0.30:
                topics.append(columns[i])
        topics_list.append(topics)
    return topics_list


@inside_workspace()
def interpret_data(ds, ts, dag: DAG, **kwargs):
    """
    Make inference for realtime data
    :param ds:
    :param kwargs:
    :return:
    """
    with open(const.INFERENCE_RESULT, 'rt') as f:
        request_result = json.load(f)
    with open(const.REAL_RESULT, 'rt') as f:
        real_result = json.load(f)
    with open(const.INFERENCE_HEADERS, 'rt') as f:
        headers: Dict = json.load(f)

    predicted = interpret(request_result)

    wrong_feedback = []
    for real, pred_topics in zip(real_result['data'], predicted):
        _, real_topics = real
        if pred_topics != real_topics:
            wrong_feedback.append(real)

    with open(const.PREPARED_FEEDBACK, 'wt') as f:
        json.dump({
            'headers': {
                'request-id': headers.get('request-id'),
                'model-name': headers.get('model-name'),
                'model-version': headers.get('model-version')
            },
            'feedback': {'data': wrong_feedback}
        }, f)


if __name__ == '__main__':

    class MockDag:
        dag_id = 'local_dag_inference'

    current_ts = 'local_ts'
    logger.info(f'current ts: {current_ts}')
    interpret_data(None, current_ts, MockDag)
