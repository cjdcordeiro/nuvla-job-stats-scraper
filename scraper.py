#!/usr/bin/env python3

import argparse
import time
import logging
from nuvla.api import Api 
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

DEF_NUVLA_URL='nuvla.io'
DEF_FREQUENCY=30

def get_argument_parser():
    parser = argparse.ArgumentParser()

    parser.add_argument('--nuvla-url',      help=f'Nuvla endpoint to connect to (default: {DEF_NUVLA_URL})', default=DEF_NUVLA_URL)
    parser.add_argument('--nuvla-key',      required=True, help='Nuvla API Key Id')
    parser.add_argument('--nuvla-secret',   required=True, help='Nuvla API Key Secret')
    parser.add_argument('--pushgateway-endpoint',   required=True, help='Prometheus Pushgateway endpoint (eg. localhost:9091)')
    parser.add_argument('--nuvla-insecure', help='Do not check Nuvla certificate', default=False, action='store_true')
    parser.add_argument('--frequency',      help=f'Time, in seconds, between each collection of statistics (default: {DEF_FREQUENCY})', default=DEF_FREQUENCY)

    return parser.parse_args()

def get_job_duration_stats_by_mode(is_pull_mode, api, registry):
    if is_pull_mode:
        metric_name = 'nuvla_pull_jobs_duration'
        base_filter = 'execution-mode="pull"'
    else:
        metric_name = 'nuvla_push_jobs_duration'
        base_filter = 'execution-mode!="pull"'

    filter = f'{base_filter} and created>="now-12h" and (state="FAILED" or state="SUCCESS"'

    j = Gauge(metric_name, 
                    'Moving duration stats of executed Nuvla jobs', 
                    ['aggregation'], registry=registry)
    jobs = api.search('job', aggregation="avg:duration,max:duration,percentiles:duration", filter=filter, last=0).data
    j.labels('average-job-duration').set(jobs['aggregations']['avg:duration']['value'])
    j.labels('max-job-duration').set(jobs['aggregations']['max:duration']['value'])
    j.labels('1-percent-duration').set(jobs['aggregations']['percentiles:duration']['values']['1.0'])
    j.labels('5-percent-duration').set(jobs['aggregations']['percentiles:duration']['values']['5.0'])
    j.labels('25-percent-duration').set(jobs['aggregations']['percentiles:duration']['values']['25.0'])
    j.labels('50-percent-duration').set(jobs['aggregations']['percentiles:duration']['values']['50.0'])
    j.labels('75-percent-duration').set(jobs['aggregations']['percentiles:duration']['values']['75.0'])
    j.labels('95-percent-duration').set(jobs['aggregations']['percentiles:duration']['values']['95.0'])
    j.labels('99-percent-duration').set(jobs['aggregations']['percentiles:duration']['values']['99.0'])

    return j

if __name__ == '__main__':
    params = get_argument_parser()

    api = Api(params.nuvla_url, insecure=params.nuvla_insecure)
    api.login_apikey(params.nuvla_key, params.nuvla_secret)

    while True:
        start_cycle = time.time()
        # ---
        
        registry = CollectorRegistry()
        try:
            q = Gauge('nuvla_jobs', 
                    'Total count of Nuvla jobs', 
                    ['state', 'execution_mode'], registry=registry)
            jobs = api.search('job', aggregation="terms:state,terms:execution-mode", last=0).data
            for jstate, jcount in list(map(lambda x: (x['key'], x['doc_count']), jobs['aggregations']['terms:state']['buckets'])):
                q.labels(jstate, "na").set(jcount)
            for jmode, jcount in list(map(lambda x: (x['key'], x['doc_count']), jobs['aggregations']['terms:execution-mode']['buckets'])):
                q.labels("na", jmode).set(jcount)
        except:
            logging.exception('Unable to fetch jobs by state')

        try:
            push = get_job_duration_stats_by_mode(False, api, registry)
        except:
            logging.exception('Unable to get stats for push jobs')

        try:
            pull = get_job_duration_stats_by_mode(True, api, registry)
        except:
            logging.exception('Unable to get stats for pull jobs')

        push_to_gateway(params.pushgateway_endpoint, job='nuvla-job-stats-scraper', registry=registry)

        
        # ---
        end_cycle = time.time()
        cycle_duration = end_cycle - start_cycle

        next_cycle_in = params.frequency - int(cycle_duration)
        if next_cycle_in < 0:
            next_cycle_in = 0

        time.sleep(next_cycle_in)