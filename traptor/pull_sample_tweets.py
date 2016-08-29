from birdy.twitter import UserClient
import json
from settings import APIKEYS


client = UserClient(APIKEYS['CONSUMER_KEY'],
                    APIKEYS['CONSUMER_SECRET'],
                    APIKEYS['ACCESS_TOKEN'],
                    APIKEYS['ACCESS_TOKEN_SECRET']
                    )

response = client.api.statuses.user_timeline.get(user_id='17919972', count=1)
print json.dumps(response.data[0])
