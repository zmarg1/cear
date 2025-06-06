import requests
import json
import random

BASE_URL = "https://api.sealevelsensors.org/v1.0"

def test_api(path):
    url = BASE_URL + path
    print(f"\n==> GET {url}")
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    print(json.dumps(data, indent=2))
    return data


if __name__ == "__main__":
    # 1. Get Things
    things = test_api("/Things")
    thing = random.choice(things["value"])
    thing_id = thing["@iot.id"]
    print(f"\nSelected Thing: id={thing_id}, name={thing['name']}")

    # 2. Get Datastreams for Thing
    datastreams = test_api(f"/Things({thing_id})/Datastreams")
    datastream = random.choice(datastreams["value"])
    datastream_id = datastream["@iot.id"]
    print(f"\nSelected Datastream: id={datastream_id}, name={datastream['name']}")

    # 3. Get Observations for Datastream (get first page, pick first)
    observations = test_api(f"/Datastreams({datastream_id})/Observations?$top=1&$orderby=phenomenonTime asc")
    if not observations["value"]:
        print("No observations found!")
        exit(1)
    observation = observations["value"][0]
    observation_id = observation["@iot.id"]
    print(f"\nSelected Observation: id={observation_id}, phenomenonTime={observation['phenomenonTime']}")

    # 4. Get FeatureOfInterest for Observation
    feature_of_interest_link = observation["FeatureOfInterest@iot.navigationLink"]
    feature_of_interest_path = feature_of_interest_link.replace(BASE_URL, "")
    feature_of_interest = test_api(feature_of_interest_path)
    print("\nFeatureOfInterest test completed successfully.")