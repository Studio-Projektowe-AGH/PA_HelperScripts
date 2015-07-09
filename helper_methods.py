import jwt
import json
import time
import requests as r
import logging
import hashlib

# Logging configuration
logging.basicConfig(format='%(levelname)s : %(asctime)s %(message)s', level=logging.INFO)

HEROKU_EVENT_QUEUE_URL = 'http://goparty-eventq.herokuapp.com'


def generate_token(payload):
    return jwt.encode(payload, 'a0a2abd8-6162-41c3-83d6-1cf559b46afc', algorithm='HS256')


def send_checkin(user_token, club_id, timestamp_offset=0):
    payload = __get_generic_payload(club_id)

    resp = r.request('POST', HEROKU_EVENT_QUEUE_URL + '/events/checkin',
                     headers=__get_request_headers(user_token), data=json.dumps(payload))

    logging.info('CHECKIN({}, {}) sent with response {}.'.format(hashlib.md5(user_token).hexdigest(),
                                                               hashlib.md5(club_id).hexdigest(), resp.status_code))


def send_checkout(user_token, club_id, timestamp_offset=0):
    payload = __get_generic_payload(club_id)

    resp = r.request('POST', HEROKU_EVENT_QUEUE_URL + '/events/checkout',
                     headers=__get_request_headers(user_token), data=json.dumps(payload))

    logging.info('CHECKOUT({}, {}) sent with response {}.'.format(hashlib.md5(user_token).hexdigest(),
                                                                hashlib.md5(club_id).hexdigest(), resp.status_code))


def send_qrscan(user_token, club_id, qr_payload, timestamp_offset=0):
    payload = __get_generic_payload(club_id)
    payload['payload'] = qr_payload

    resp = r.request('POST', HEROKU_EVENT_QUEUE_URL + '/events/qrscan',
                     headers=__get_request_headers(user_token), data=json.dumps(payload))

    logging.info('QRSCAN({}, {}) sent with response {}.'.format(hashlib.md5(user_token).hexdigest(),
                                                              hashlib.md5(club_id).hexdigest(), resp.status_code))


def send_rating(user_token, club_id, rating, timestamp_offset=0):
    payload = __get_generic_payload(club_id)
    payload['rating'] = rating

    resp = r.request('POST', HEROKU_EVENT_QUEUE_URL + '/events/rating',
                     headers=__get_request_headers(user_token), data=json.dumps(payload))
    logging.info('RATING({}, {}) sent with response {}.'.format(hashlib.md5(user_token).hexdigest(),
                                                              hashlib.md5(club_id).hexdigest(), resp.status_code))


def __get_request_headers(user_token):
    return {
        'Content-type': 'text/json',
        'Authorization': 'BEARER ' + user_token
    }


def __get_generic_payload(club_id, timestamp_offset=0):
    return {
        'timestamp': int(time.time() + timestamp_offset),
        'clubId': club_id
    }