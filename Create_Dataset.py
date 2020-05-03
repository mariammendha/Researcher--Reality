'''
Take the existing Kc_house_data.csv from MongoDb Atlas, parsing for latitude and longitude
It then pipes this data through the WalkScore Api, generating 3 accessibility scores
These 3 scores are then inserted into their associated rows within MongoDb
WalkScore Api: https://www.walkscore.com/professional/walk-score-apis.php
'''

# Function returns 3 scores (Walk, Transit, Bike)
# Scored out of 100
def rating(address, latitude, longitude):
    from walkscore import WalkScoreAPI
    api_key = '<Api Key>'
    walkscore_api = WalkScoreAPI(api_key=api_key)
    result = walkscore_api.get_score(latitude, longitude, address)
    return (result.walk_score, result.transit_score, result.bike_score)


import pymongo

# Database connection string

db = \
    pymongo.MongoClient('mongodb+srv://<username>:<password>@housing-peu0h.gcp.mongodb.net/test?retryWrites=true&w=majority'
                        )

# Declare collection

collection = db.Parameters

# Declare Document

data = collection.Housing

# Loop through each existing row, piping data through the rating api
# and populating 3 new fields (walk_score, transit_score, bike_score)

for i in data.find():
    ratinglist = rating('', i['lat'], i['long'])
    (x, y, z) = (ratinglist[0], ratinglist[1], ratinglist[2])
    data.update_many({'id': i['id']}, {'$set': {'walk_score': x,
                     'transit_score': y, 'bike_score': z}})
    count += 1
