import json

from beir import util, LoggingHandler
from beir.retrieval import models
from beir.datasets.data_loader import GenericDataLoader
from beir.retrieval.evaluation import EvaluateRetrieval
from beir.retrieval.search.dense import DenseRetrievalExactSearch as DRES

import logging
import pathlib, os
import random

#### Just some code to print debug information to stdout
logging.basicConfig(format='%(asctime)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO,
                    handlers=[LoggingHandler()])
#### /print debug information to stdout

# dataset = "msmarco"
BEIR_datasets = [
        # 'trec-covid', 'nfcorpus',
        # 'fiqa', 'arguana', 'webis-touche2020',
        # 'scidocs', 'scifact',
        # 'quora',
        # 'nq', 'hotpotqa',
        'dbpedia-entity',
        # 'fever',
        # 'climate-fever',
        # 'msmarco',
        # 'cqadupstack', # this is special
        ]

for dataset in BEIR_datasets:
    print('*' * 30)
    print(dataset)
    print('*' * 30)
    output_dir = '/export/home/exp/search/ANCE/beir_eval/beir_released.passage_ANCE_firstp.dot'
    split = 'dev' if dataset == 'msmarco' else 'test'
    score_function = 'dot'
    #### Download NFCorpus dataset and unzip the dataset
    url = "https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/{}.zip".format(dataset)
    out_dir = os.path.join(pathlib.Path(__file__).parent.absolute(), "datasets")
    data_path = util.download_and_unzip(url, out_dir)

    #### Provide the data path where nfcorpus has been downloaded and unzipped to the data loader
    # data folder would contain these files:
    # (1) nfcorpus/corpus.jsonl  (format: jsonlines)
    # (2) nfcorpus/queries.jsonl (format: jsonlines)
    # (3) nfcorpus/qrels/test.tsv (format: tsv ("\t"))

    corpus, queries, qrels = GenericDataLoader(data_folder=data_path).load(split=split)

    #### Dense Retrieval using ANCE ####
    # https://www.sbert.net/docs/pretrained-models/msmarco-v3.html
    # MSMARCO Dev Passage Retrieval ANCE(FirstP) 600K model from ANCE.
    # The ANCE model was fine-tuned using dot-product (dot) function.

    model = DRES(models.SentenceBERT("msmarco-roberta-base-ance-firstp"))
    retriever = EvaluateRetrieval(model, score_function=score_function)

    #### Retrieve dense results (format of results is identical to qrels)
    results = retriever.retrieve(corpus, queries)

    #### Evaluate your retrieval using NDCG@k, MAP@K ...

    logging.info("Retriever evaluation for k in: {}".format(retriever.k_values))
    ndcg, _map, recall, precision = retriever.evaluate(qrels, results, retriever.k_values)

    #### Print top-k documents retrieved ####
    top_k = 10

    query_id, ranking_scores = random.choice(list(results.items()))
    scores_sorted = sorted(ranking_scores.items(), key=lambda item: item[1], reverse=True)
    logging.info("Query : %s\n" % queries[query_id])

    for rank in range(top_k):
        doc_id = scores_sorted[rank][0]
        # Format: Rank x: ID [Title] Body
        logging.info("Rank %d: %s [%s] - %s\n" % (rank+1, doc_id, corpus[doc_id].get("title"), corpus[doc_id].get("text")))


    print(f"Writing scores to {output_dir + '/' + dataset}.json")
    result_dict = {
        'dataset': dataset,
        'split': split,
        'metric': score_function,
        'scores': {
            'ndcg': ndcg,
            'map': _map,
            'precision': precision,
            'recall': recall,
        }
    }

    os.makedirs(output_dir, exist_ok=True)
    with open(f"{output_dir + '/' + dataset}.json", 'w') as writer:
        writer.write(json.dumps(result_dict, indent=4) + "\n")

    print(f"Writing scores to {output_dir + '/' + dataset}.csv")
    rows = ['metric,@1,@3,@5,@10,@100,@1000']
    for metric, scores in result_dict['scores'].items():
        row = ','.join([str(s) for s in ([metric] + list(scores.values()))])
        rows.append(row)
    with open(f"{output_dir + '/' + dataset}.csv", 'w') as writer:
        for row in rows:
            writer.write(row + "\n")
