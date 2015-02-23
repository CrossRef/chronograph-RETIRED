import requests

PUSH_API_URL="http://localhost:3000/api/push"
PUSH_TYPE="WikipediaCitation"
PUSH_SOURCE="Cocytus"
PUSH_TOKEN="ba48f88b0bb0b3f8436e340ff6910a0f"


action="add"
article_url="http://en.wikipedia.org/wiki/Fish"
timestamp=123456789
doi="10.5555/12345678"

print "Push..."
resp = requests.post(PUSH_API_URL, json={"doi": doi,
                                  "source": PUSH_SOURCE,
                                  "type": PUSH_TYPE,
                                  "arg1":action,
                                  "arg2":article_url,
                                  "arg3":timestamp}, headers= {"Token": PUSH_TOKEN})

print resp
print resp.text