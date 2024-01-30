import csv
import logging

import redis

import os
from dotenv import load_dotenv

load_dotenv()

# Constants
REDIS_HOST = os.getenv('REDIS_HOST', default="redismicro.platformengineer.io")
REDIS_PORT = os.getenv('REDIS_PORT', default=6379)
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', default="<PASSWORD>")

CSV_FILE_PATH = "external_resources/worldcities.csv"
INDEX_HASH_KEY = "idx:city_by_name"
GEO_INDEX_HASH_KEY = "idx:cities"
BREWERY_CSV_FILE_PATH = "external_resources/breweries.csv"
GEO_BREWERY_INDEX_HASH_KEY = "idx:breweries"

# Logger Configuration
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


def create_redis_client(host, port, password, decode_responses=False):
    return redis.Redis(host=host, port=port, db=0, password=password, decode_responses=decode_responses)


def format_city_hash_key(city_id):
    return f"city:{city_id}"


def format_brewery_hash_key(brewery_id):
    return f"brw:{brewery_id}"


def parse_city_data(csv_row):
    return {
        "_id": csv_row['id'],
        "name": csv_row['city'],
        "ascii_name": csv_row['city_ascii'],
        "latitude": csv_row['lat'],
        "longitude": csv_row['lng'],
        "country": csv_row['country'],
        "iso3": csv_row['iso3'],
        "admin_name": csv_row['admin_name'],
        "capital": csv_row['capital'],
        "population": csv_row['population']
    }


def parse_brewery_data(csv_row):
    return {
        "_id": csv_row['id'],
        "name": csv_row['breweries'],
        "city": csv_row['city'],
        "state": csv_row['state'],
        "code": csv_row['code'],
        "country": csv_row['country'],
        "phone": csv_row['phone'],
        "website": csv_row['website']
    }


def store_city_data_in_redis(redis_client, city_id, city_data):
    try:
        redis_client.hset(format_city_hash_key(city_id), mapping=city_data)
        logger.info(f"Stored data for city ID: {city_id}")
    except redis.RedisError as e:
        logger.error(f"Error storing city data in Redis: {e}")


def store_brewery_data_in_redis(redis_client, brewery_id, brewery_data):
    try:
        redis_client.hset(format_brewery_hash_key(brewery_id), mapping=brewery_data)
        logger.info(f"Stored data for city ID: {brewery_id}")
    except redis.RedisError as e:
        logger.error(f"Error storing city data in Redis: {e}")


def create_full_world_cities_index_as_redis_hash(redis_client, index_hash_name, raw_csv_file):
    logger.info(f"Creating a Index via Hash in Redis...")
    logger.info(f"Index Hash Key: {index_hash_name} - CSV File: {raw_csv_file}")
    index_world_cities_dict = {}

    try:
        with open(raw_csv_file, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                city_id = row.get('id')
                city_name = row.get('city')
                if city_id:
                    logger.info(f"Processing index for city ID: {city_id} - city name: {city_name}")
                    index_world_cities_dict[city_name] = city_id
                else:
                    logger.warning("Missing city ID in row:", row)

        redis_client.hset(index_hash_name, mapping=index_world_cities_dict)
    except FileNotFoundError:
        logger.error(f"CSV file not found: {CSV_FILE_PATH}")
    except Exception as e:
        logger.error(f"An error occurred: {e}")


def create_geospatial_index_as_redis_geo(redis_client, geo_index_key, raw_csv_file):
    logger.info(f"Creating the Geo Index for the World Cities in Redis...")
    logger.info(f"Geo Index Hash Key: {geo_index_key} - CSV File: {raw_csv_file}")

    try:
        with open(raw_csv_file, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                city_id = row.get('id')
                latitude = row.get('lat')
                longitude = row.get('lng')
                if city_id and latitude and longitude:
                    logger.info(f"Adding city {city_id} to Geo Index")
                    redis_client.geoadd(geo_index_key, (longitude, latitude, city_id))
                else:
                    logger.warning(f"Missing data for city ID {city_id}: {row}")
    except FileNotFoundError:
        logger.error(f"CSV file not found: {CSV_FILE_PATH}")
    except Exception as e:
        logger.error(f"An error occurred: {e}")


def create_brewery_geospatial_index_as_redis_geo(redis_client, brewery_geo_index_key, brewery_raw_csv_file):
    logger.info(f"Creating the Geo Index for the Breweries in Redis...")
    logger.info(f"Brewery Geo Index Hash Key: {brewery_geo_index_key} - CSV File: {brewery_raw_csv_file}")

    try:
        with open(brewery_raw_csv_file, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile, delimiter=';')
            for row in reader:
                brewery_id = row.get('id')
                coordinates = row.get('coordinates', '').split(",")
                if brewery_id and len(coordinates) == 2:
                    latitude, longitude = coordinates
                    logger.info(
                        f"Adding city {brewery_id} to Geo Index - lat: {latitude} - long: {longitude} - latlong: {row.get('coordinates')}")
                    redis_client.geoadd(brewery_geo_index_key, (longitude, latitude, brewery_id))
                else:
                    logger.warning(f"Missing data for Brewery ID {brewery_id}: {row}")
    except FileNotFoundError:
        logger.error(f"CSV file not found: {CSV_FILE_PATH}")
    except Exception as e:
        logger.error(f"An error occurred: {e}")


def ex_2_scan_for_a_city(redis_client, city_name):
    # First we get the City ID
    city_id_results_from_idx = redis_client.hget(INDEX_HASH_KEY, city_name).decode('utf-8')
    logger.info(f"City ID from Search: {city_id_results_from_idx}")

    # Then we get the City Entity Object from Redis
    city_id_in_redis = format_city_hash_key(city_id_results_from_idx)
    logger.info(f"city_id_in_redis: {city_id_in_redis}")
    city_complete_obj = redis_client.hgetall(city_id_in_redis)
    logger.info(f"city_complete_obj: {city_complete_obj}")

    return city_complete_obj

# I know the lat long is avail in the original obj, but Ill use the index to test the GEOPOS command
def ex_3_retrieve_country_and_coordinates_of_a_city(redis_client, city_name):
    # First we get the City ID
    city_id_results_from_idx = redis_client.hget(INDEX_HASH_KEY, city_name)
    logger.info(f"City ID from Search: {city_id_results_from_idx}")

    # Then we get the City Entity Object from Redis
    city_id_in_redis = format_city_hash_key(city_id_results_from_idx)
    logger.info(f"city_id_in_redis: {city_id_in_redis}")
    city_complete_obj = redis_client.hgetall(city_id_in_redis)

    # method with geopos
    city_lat_long = redis_client.geopos(GEO_INDEX_HASH_KEY, city_id_results_from_idx)


    return {
        "name": city_complete_obj['name'],
        "latitude": city_complete_obj['latitude'],
        "longitude": city_complete_obj['longitude'],
        "geopos_lat_long": city_lat_long,
        "country": city_complete_obj['country']
    }


def ex_4_and_5_retrieve_top_10_closest_breweries_of_a_city(redis_client, city_name):

    curated_list_of_closest_10_breweries = []

    # First we get the City ID
    city_id_results_from_idx = redis_client.hget(INDEX_HASH_KEY, city_name)
    logger.info(f"City ID from Search: {city_id_results_from_idx}")

    # Then we get the city's geopos
    city_long, city_lat = redis_client.geopos(GEO_INDEX_HASH_KEY, city_id_results_from_idx)[0]


    # Then we use georadius to hit the geopos brewery index -  - deprecated - so its a combo of GEOSEARCH + BYRADIUS + COUNT
    logger.info(city_long)
    logger.info(city_lat)

    closest_breweries = redis_client.geosearch(GEO_BREWERY_INDEX_HASH_KEY, longitude=city_long, latitude=city_lat, unit="km", radius=1000, count=10, sort="ASC")
    logger.info(closest_breweries)

    # Finally, we get the ids and build the final list containing the top 10 closest breweries
    for brewery_id in closest_breweries:
        brewery_id_in_redis_key_format = format_brewery_hash_key(brewery_id)
        brewery_details_obj = redis_client.hgetall(brewery_id_in_redis_key_format)
        curated_list_of_closest_10_breweries.append(brewery_details_obj)

    return curated_list_of_closest_10_breweries

def main():
    redis_client = create_redis_client(REDIS_HOST, REDIS_PORT, REDIS_PASSWORD, decode_responses=True)
    # Creating the hash index - city name and id
    # create_full_world_cities_index_as_redis_hash(redis_client, INDEX_HASH_KEY, CSV_FILE_PATH)

    # Creating the GEO index - city id lat long
    # create_geospatial_index_as_redis_geo(redis_client, GEO_INDEX_HASH_KEY, CSV_FILE_PATH)

    # Creating the GEO index for Breweries - brewery id lat long
    # create_brewery_geospatial_index_as_redis_geo(redis_client, GEO_BREWERY_INDEX_HASH_KEY, BREWERY_CSV_FILE_PATH)

    # Exercise 2 -
    #ex_2_scan_for_a_city(redis_client, "Belo Horizonte")

    # Exercise 3 -
    #result = ex_3_retrieve_country_and_coordinates_of_a_city(redis_client, "Belo Horizonte")
    #logger.info(f"EXERCISE 3 RESULTS: {result}")

    # Exercise 4 -
    result = ex_4_and_5_retrieve_top_10_closest_breweries_of_a_city(redis_client, "London")
    logger.info(f"EXERCISE 4 RESULTS: {result}")


    # THIS IS TO LOAD THE WORLD CITIES - v0.0.1
    '''
    try:
        with open(CSV_FILE_PATH, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                city_id = row.get('id')
                if city_id:
                    logger.info(f"Processing city ID: {city_id}")
                    city_data = parse_city_data(row)
                    store_city_data_in_redis(redis_client, city_id, city_data)
                else:
                    logger.warning("Missing city ID in row:", row)
    except FileNotFoundError:
        logger.error(f"CSV file not found: {CSV_FILE_PATH}")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
    
    # THIS IS TO LOAD THE BREWERIES - v0.0.1
    try:
        with open(BREWERY_CSV_FILE_PATH, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile, delimiter=';')
            for row in reader:
                brewery_id = row.get('id')
                if brewery_id:
                    logger.info(f"Processing Brewery ID: {brewery_id}")
                    brewery_data = parse_brewery_data(row)
                    logger.info(brewery_data)
                    store_brewery_data_in_redis(redis_client, brewery_id, brewery_data)
                else:
                    logger.warning(f"Missing Brewery ID in row: {row}")
    except FileNotFoundError:
        logger.error(f"CSV file not found: {CSV_FILE_PATH}")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
    '''


if __name__ == '__main__':
    logger.info("Starting main function...")
    main()
    logger.info("Main function complete!")
    logger.info("Bye!")
