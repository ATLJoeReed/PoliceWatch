#!/usr/bin/python3.6
# -*- coding: utf-8 -*-
import os
import sys

import utils


def process_incidents(logger):
    try:
        conn, cur, dict_cur = utils.get_database_connection()
    except Exception as e:
        logger.error("Getting database connection")
        sys.exit("Unable to get database connection")

    url = utils.build_extract_url(logger)
    logger.info("Starting the extract")
    results = utils.extract_data(url, logger)
    number_results = len(results)
    logger.info(f"Extracted {number_results} records")
    load_status = utils.load_data(conn, cur, results, logger)
    logger.info(f"Load status: {load_status}")
    if load_status == 'success':
        incidents = utils.get_new_incidents(dict_cur)
        number_incidents = len(incidents)
        logger.info(f"Found {number_incidents} incidents")
        if number_results > 0:
            api = utils.get_twitter_auth()
            for incident in incidents:
                tweet_success = utils.update_status(api, incident, conn, cur)
                if tweet_success:
                    logger.info("Tweet status posted successfully")
                else:
                    logger.error("Posting tweet status")
    conn.close()
    cur.close()
    dict_cur.close()


if __name__ == '__main__':
    # move to working directory...
    abspath = os.path.abspath(__file__)
    dname = os.path.dirname(abspath)
    os.chdir(dname)

    logger = utils.setup_logger_stdout('process_incidents')

    process_incidents(logger)
