#!/usr/bin/python3.7
# -*- coding: utf-8 -*-
import datetime
import logging
import sys

import psycopg2
import psycopg2.extras
import requests
import tweepy

from config import constraint_sql, settings


def build_google_map_url(latitude, longitude):
    return f"https://www.google.com/maps/search/?api=1&query={latitude},{longitude}" # noqa


def build_extract_url(logger):
    start_date, end_date = get_date_range()
    base_url = settings.SOCRATA_BASE_URL
    url = f"{base_url}$where=date_trunc_ymd(updated_at) between '{start_date}' and '{end_date}'" # noqa
    logger.info(f"Extract URL: {url}")
    return url


def build_tweet(tweet_data):
    incident_id = tweet_data.get('incident_id')
    case_number = tweet_data.get('case_number')
    incident_datetime = tweet_data.get('incident_datetime')
    incident_description = tweet_data.get('incident_description')
    address_1 = tweet_data.get('address_1')

    url = build_google_map_url(
        tweet_data.get('latitude'),
        tweet_data.get('longitude')
    )
    tiny_url = get_tiny_url(url)

    tweet = f"Incident ID: {incident_id}\nCase Number: {case_number}\n" \
        f"Incident Datetime: {incident_datetime}\n" \
        f"Incident Description: {incident_description}\n" \
        f"Incident Location: {address_1}\n\n{tiny_url}"
    return tweet


def check_results(data, fields):
    fields = fields.replace('[', '').replace(']', '').replace('"','').split(',') # noqa
    for row in data:
        for field in fields:
            if field not in row.keys():
                row[field] = None
    return data


def extract_data(url, logger):
    headers = {"X-App-Token": settings.SOCRATA_API_TOKEN}
    results = requests.get(url, headers=headers)
    status_code = results.status_code
    if status_code == 200:
        return check_results(results.json(), results.headers['X-SODA2-Fields'])
    else:
        logger.error(f"Making request - status code: {status_code}")
        return None


def get_database_connection():
    conn = psycopg2.connect(**settings.DB_CONN)
    cur = conn.cursor()
    dict_cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    return (conn, cur, dict_cur)


def get_date_range():
    today = datetime.datetime.today()
    start_date = today - datetime.timedelta(14)
    end_date = today + datetime.timedelta(2)
    return (
        datetime.datetime.strftime(start_date, '%Y-%m-%d'),
        datetime.datetime.strftime(end_date, '%Y-%m-%d'),
        )


def get_new_incidents(dict_cursor):
    dict_cursor.execute(constraint_sql.SELECT_NEW_INCIDENTS_SQL)
    return dict_cursor.fetchall()


def get_tiny_url(url):
    tiny_url = f"http://tinyurl.com/api-create.php?url={url}"
    results = requests.get(tiny_url)
    if results.status_code == 200:
        return results.text
    else:
        return url


def get_twitter_auth():
    auth = tweepy.OAuthHandler(
        settings.TWITTER_CONSUMER_TOKEN,
        settings.TWITTER_CONSUMER_SECRET
    )
    auth.set_access_token(settings.TWITTER_KEY, settings.TWITTER_SECRET)
    return tweepy.API(auth)


def load_data(conn, cur, data, logger):
    try:
        cur.execute("truncate table raw.east_point_incidents;")
        conn.commit()
        cur.executemany(constraint_sql.INCIDENT_INSERT_SQL, data)
        conn.commit()
        cur.execute(constraint_sql.INCIDENT_MERGE_SQL)
        conn.commit()
        return "success"
    except Exception as e:
        logger.error("Loading data into database")
        return "failure"


def setup_logger_stdout(logger_name):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger


def update_status(api, tweet_data, conn, cur):
    tweet = build_tweet(tweet_data)
    tweet_data['length_tweet'] = len(tweet)
    latitude = tweet_data.get('latitude')
    longitude = tweet_data.get('longitude')
    results = api.update_status(tweet, lat=latitude, long=longitude)
    if results.id:
        cur.execute(constraint_sql.TWEET_SENT_INSERT_SQL, tweet_data)
        conn.commit()
        return True
    else:
        return False
