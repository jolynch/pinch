pinch-server
============
The pinch server provides UNIX pipes and streaming HTTP endpoints for
compressing and decompressing data either via a local sidecar (read and write
to well defined files) or HTTP (read and write to a streaming HTTP interface)

Example bi-directional compression and decompression:
```bash
# Setup the pinch and unpinch pipelines, read out the handles
FD=$(curl -s 'localhost:8080/pinch?min-level=1&timeout=2m' | jq '.handles | keys[0]' -r)
RFD=$(curl -s 'localhost:8080/unpinch?timeout=200s' | jq '.handles | keys[0]' -r)

# Round trip compress and decompress
curl -svT yelp_academic_dataset_review.json "localhost:8080/io/${FD}" | curl -svT - "localhost:8080/io/${RFD}" | wc -c

# Check the status
curl "localhost:8080/status/${FD} | jq ."
```

There is also a pure file API available on the local server via control
pipes:
```
# Step 1: Setup the pipeline you want

$ PIPELINE=$(curl -s 'localhost:8080/pinch?min-level=1&timeout=2m')
$ echo $PIPELINE | jq .
{
  "handles": {
    "d11bece37bef8d8b": {
      "io-http": "http://127.0.0.1:8080/io/d11bece37bef8d8b",
      "in-pipe": "/run/pinch/in/d11bece37bef8d8b",
      "out-pipe": "/run/pinch/out/d11bece37bef8d8b"
    }
  },
  "compression": {
    "algorithm": "zstd:adapt",
    "extension": "zst",
    "max-level": 10,
    "min-level": 1
  },
  "encryption": {
    "algorithm": "plaintext"
  },
  "time-to-live": 120000000000
}

$ FD=$(echo $PIPELINE | jq '.handles | keys[0]' -r)

# For writing data into the pipeline
$ IN=$(echo $PIPELINE | jq --arg FD "$FD" '.handles | values[$FD]["in-pipe"]' -r)

# For reading out of the pipeline
$ OUT=$(echo $PIPELINE | jq --arg FD "$FD" '.handles | values[$FD]["out-pipe"]' -r)

# Now write some data to the pipe and read it back
$ time dd if=/dev/zero bs=16K count=16384 > $IN && echo "c" > $CTRL & cat $OUT | wc -c
```
Or for easy copy-paste

```
PIPELINE=$(curl -s 'localhost:8080/pinch?min-level=1&timeout=2m')
FD=$(echo $PIPELINE | jq '.handles | keys[0]' -r)
IN=$(echo $PIPELINE | jq --arg FD "$FD" '.handles | values[$FD]["in-pipe"]' -r)
OUT=$(echo $PIPELINE | jq --arg FD "$FD" '.handles | values[$FD]["out-pipe"]' -r)

time dd if=/dev/zero bs=16K count=16384 > $IN && curl -XPUT localhost:8080/io/${FD} & cat $OUT | wc -c

curl localhost:8080/status/${FD} | jq .
```

Compression and Encryption
==========================

Pinch can also encrypt and decrypt data using `age`.

```bash
$ curl -s 'localhost:8080/pinch?timeout=100s&age-public-key=age1630vztsaydze8r9qc3e865spc989mvcls3wg7hh9vu2w3luulqlqp0v6wh' | jq .
{
  "handles": {
    "14ccbb285f4d5822": {
      "io-http": "http://127.0.0.1:8080/io/14ccbb285f4d5822",
      "in-pipe": "/run/pinch/in/14ccbb285f4d5822",
      "out-pipe": "/run/pinch/out/14ccbb285f4d5822"
    }
  },
  "compression": {
    "algorithm": "zstd:adapt",
    "extension": "zst",
    "max-level": 10
  },
  "encryption": {
    "algorithm": "age:chacha20poly1305",
    "extension": "age",
    "public-key": "age1630vztsaydze8r9qc3e865spc989mvcls3wg7hh9vu2w3luulqlqp0v6wh"
  },
  "ttl": 200
}

# Let's compress and encrypt a file
$ curl -sT yelp_academic_dataset_business.json 'localhost:8080/io/14ccbb285f4d5822' > business.zst.age

# And now let's decompress and decrypt it using the private key stored in
# /run/pinch/keys/private

$ curl -s 'localhost:8080/unpinch?age-key-path=private&timeout=200s' | jq .
{
  "handles": {
    "0d2eeb7e0819c098": {
      "io-http": "http://127.0.0.1:8080/io/0d2eeb7e0819c098",
      "in-pipe": "/run/pinch/in/0d2eeb7e0819c098",
      "out-pipe": "/run/pinch/out/0d2eeb7e0819c098"
    }
  },
  "ttl": 200
}

$ curl -s 'localhost:8080/unpinch?age-key-path=private&timeout=200' | jq .
{
  "handles": {
    "1805b02fce8ecca1": {
      "io-http": "http://127.0.0.1:8080/io/1805b02fce8ecca1",
      "in-pipe": "/run/pinch/in/1805b02fce8ecca1",
      "out-pipe": "/run/pinch/out/1805b02fce8ecca1"
    }
  },
  "ttl": 200
}


$ curl -sT business.zst.age 'localhost:8080/io/1805b02fce8ecca1' | head
{"business_id":"6iYb2HFDywm3zjuRg0shjw","name":"Oskar Blues Taproom","address":"921 Pearl St","city":"Boulder","state":"CO","postal_code":"80302","latitude":40.0175444,"longitude":-105.2833481,"stars":4.0,"review_count":86,"is_open":1,"attributes":{"RestaurantsTableService":"True","WiFi":"u'free'","BikeParking":"True","BusinessParking":"{'garage': False, 'street': True, 'validated': False, 'lot': False, 'valet': False}","BusinessAcceptsCreditCards":"True","RestaurantsReservations":"False","WheelchairAccessible":"True","Caters":"True","OutdoorSeating":"True","RestaurantsGoodForGroups":"True","HappyHour":"True","BusinessAcceptsBitcoin":"False","RestaurantsPriceRange2":"2","Ambience":"{'touristy': False, 'hipster': False, 'romantic': False, 'divey': False, 'intimate': False, 'trendy': False, 'upscale': False, 'classy': False, 'casual': True}","HasTV":"True","Alcohol":"'beer_and_wine'","GoodForMeal":"{'dessert': False, 'latenight': False, 'lunch': False, 'dinner': False, 'brunch': False, 'breakfast': False}","DogsAllowed":"False","RestaurantsTakeOut":"True","NoiseLevel":"u'average'","RestaurantsAttire":"'casual'","RestaurantsDelivery":"None"},"categories":"Gastropubs, Food, Beer Gardens, Restaurants, Bars, American (Traditional), Beer Bar, Nightlife, Breweries","hours":{"Monday":"11:0-23:0","Tuesday":"11:0-23:0","Wednesday":"11:0-23:0","Thursday":"11:0-23:0","Friday":"11:0-23:0","Saturday":"11:0-23:0","Sunday":"11:0-23:0"}}
{"business_id":"tCbdrRPZA0oiIYSmHG3J0w","name":"Flying Elephants at PDX","address":"7000 NE Airport Way","city":"Portland","state":"OR","postal_code":"97218","latitude":45.5889058992,"longitude":-122.5933307507,"stars":4.0,"review_count":126,"is_open":1,"attributes":{"RestaurantsTakeOut":"True","RestaurantsAttire":"u'casual'","GoodForKids":"True","BikeParking":"False","OutdoorSeating":"False","Ambience":"{'romantic': False, 'intimate': False, 'touristy': False, 'hipster': False, 'divey': False, 'classy': False, 'trendy': False, 'upscale': False, 'casual': True}","Caters":"True","RestaurantsReservations":"False","RestaurantsDelivery":"False","HasTV":"False","RestaurantsGoodForGroups":"False","BusinessAcceptsCreditCards":"True","NoiseLevel":"u'average'","ByAppointmentOnly":"False","RestaurantsPriceRange2":"2","WiFi":"u'free'","BusinessParking":"{'garage': True, 'street': False, 'validated': False, 'lot': False, 'valet': False}","Alcohol":"u'beer_and_wine'","GoodForMeal":"{'dessert': False, 'latenight': False, 'lunch': True, 'dinner': False, 'brunch': False, 'breakfast': True}"},"categories":"Salad, Soup, Sandwiches, Delis, Restaurants, Cafes, Vegetarian","hours":{"Monday":"5:0-18:0","Tuesday":"5:0-17:0","Wednesday":"5:0-18:0","Thursday":"5:0-18:0","Friday":"5:0-18:0","Saturday":"5:0-18:0","Sunday":"5:0-18:0"}}
```

