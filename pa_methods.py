from multiprocessing.dummy import Pool as ThreadPool
import time
import helper_methods as hm
import pymongo as pm
import random
import logging

# Logging configuration
logging.basicConfig(format='%(levelname)s : %(asctime)s %(message)s', level=logging.INFO)


mongo_client = pm.MongoClient('mongodb://app:rogus2015!@ds037601.mongolab.com:37601/goparty')
mongo_database = mongo_client.goparty

user_collection = mongo_database.users
business_collection = mongo_database.businessUsers
individual_profile_collection = mongo_database.individualUserProfiles
business_profile_collection = mongo_database.businessUserProfiles


def get_business_id_set():
    business_id_cursors = business_profile_collection.find(projection=['_id'])
    business_id_list = map(lambda object: str(object['_id']), business_id_cursors)
    return set(business_id_list)


def get_user_token_set():
    user_data_cursor = user_collection.find(projection=['_id', 'role'])
    user_token_list = map(lambda object: hm.generate_token({'userId': str(object['_id']), 'userRole': object['role']}), user_data_cursor)
    return set(user_token_list)


def match_users_with_clubs(user_token_set, business_id_set, clubs_per_user):
    return [(user_token, draw_random_ids(business_id_set, clubs_per_user)) for user_token in user_token_set]


def draw_random_ids(id_set, count):
    return random.sample(id_set, count)


def simulate_visit((user_token, business_id)):
    hm.send_checkin(user_token, business_id)
    for _ in xrange(1, random.randint(0, 5)):
        hm.send_qrscan(user_token, business_id, 'sample_payload')
    hm.send_rating(user_token, business_id, random.randint(1, 5))
    hm.send_checkout(user_token, business_id, timestamp_offset=random.randint(0, 3600))


def simulate_active_visits(users_tokens, business_id, visit_time, isRatingVisit=False):
    map(lambda user_token: hm.send_checkin(user_token, business_id), users_tokens)

    if isRatingVisit:
        map(lambda user_token: hm.send_rating(user_token, business_id, 5), users_tokens)
    else:
        map(lambda user_token: hm.send_rating(user_token, business_id, random.randint(1, 5)), users_tokens)

    time.sleep(visit_time)
    map(lambda user_token: hm.send_checkout(user_token, business_id), users_tokens)


def fill_db(clubs_per_user, thread_count=4):
    user_set = get_user_token_set()
    business_set = get_business_id_set()

    matched_objects = match_users_with_clubs(user_set, business_set, clubs_per_user)
    transformed_list = ((user_token, business) for (user_token, business_ids) in matched_objects for business in business_ids)

    # Multithreaded sending
    pool = ThreadPool(thread_count)
    pool.map(simulate_visit, transformed_list)
    pool.close()
    pool.join()


def choose_test_sets(user_count=25, club_count=1):
    business_id_set = get_business_id_set()
    user_id_set = get_user_token_set()

    tested_business_id = draw_random_ids(business_id_set, 1)
    chosen_user_ids = draw_random_ids(user_id_set, user_count)
    return tested_business_id, chosen_user_ids


def random_club_average_rating(user_count=25, sleep_time=5):
    chosen_business_id, chosen_user_tokens = choose_test_sets(user_count)
    simulate_active_visits(chosen_user_tokens, chosen_business_id[0], sleep_time, isRatingVisit=True)


def random_club_occupation_level(user_count=25, sleep_time=5):
    chosen_business_id, chosen_user_tokens = choose_test_sets(user_count)
    simulate_active_visits(chosen_user_tokens, chosen_business_id[0], sleep_time)


if __name__ == "__main__":
    # fill_db(5)
    # random_club_average_rating()
    random_club_occupation_level()