#!/bin/bash

# Starts localstack docker container for local testing

docker run --rm \
	   -d \
	   -p 4566:4566 \
	   -p 4571:4571 \
	   --name localstack \
	   -e SERVICES="dynamodb" \
	   localstack/localstack

echo "Waiting for localstack to start"
until grep "Ready" <(docker logs localstack) > /dev/null; do
    printf '.'
    sleep 0.5
done
echo
echo "Creating tables"

# Create tables
aws --endpoint-url http://localhost:4566 dynamodb create-table \
	--table-name MusicCollection \
	--attribute-definitions AttributeName=Artist,AttributeType=S AttributeName=AlbumTitle,AttributeType=S \
	--key-schema AttributeName=Artist,KeyType=HASH AttributeName=AlbumTitle,KeyType=RANGE \
	--provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5 > /dev/null

aws --endpoint-url http://localhost:4566 dynamodb create-table \
	--table-name FilmCollection \
	--attribute-definitions AttributeName=Name,AttributeType=S \
	--key-schema AttributeName=Name,KeyType=HASH \
	--provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5 > /dev/null

echo "Inserting items"

# Insert test records
aws --endpoint-url http://localhost:4566 dynamodb put-item \
    --table-name MusicCollection \
    --item '{
        "Artist": {"S": "Metallica"},
        "AlbumTitle": {"S": "Master of Puppets"},
        "Genre": {"S": "Thrash metal"},
	"Year": {"N": "1986"},
	"Personnel": { "M": {
		"James Hetfield": {"SS": ["rhythm guitar", "vocals"]},
		"Lars Ulrich": {"SS": ["drums"]},
		"Cliff Burton": {"SS": ["bass", "backing vocals"]},
		"Kirk Hammett": {"SS": ["lead guitar"]}
	}}
      }' > /dev/null

aws --endpoint-url http://localhost:4566 dynamodb put-item \
    --table-name MusicCollection \
    --item '{
        "Artist": {"S": "Wintersun"},
        "AlbumTitle": {"S": "Wintersun"},
        "Genre": {"S": "Melodic death metal"},
	"Year": {"N": "2004"},
	"Personnel": { "M": {
		"Jari Mäenpää": {"SS": ["vocals", "guitar", "bass", "keyboards"]},
		"Kai Hahto": {"SS": ["drums"]},
		"Teemu Mäntysaari": {"SS": ["guitar"]},
		"Jukka Koskinen": {"SS": ["bass"]}
	}}
      }' > /dev/null

aws --endpoint-url http://localhost:4566 dynamodb put-item \
    --table-name MusicCollection \
    --item '{
        "Artist": {"S": "Children of Bodom"},
        "AlbumTitle": {"S": "Follow the Reaper"},
        "Genre": {"S": "Melodic death metal"},
	"Year": {"N": "2000"},
	"Songs": {"L": [ {"S": "Follow the Reaper"}, {"S": "Mask of Sanity"}, {"S": "Kissing the Shadows"} ]},
	"Personnel": { "M": {
		"Alexi Laiho": {"SS": ["lead guitar", "vocals"]},
		"Alexander Kuoppala": {"SS": ["rhythm guitar"]},
		"Janne Wirman": {"SS": ["keyboards"]},
		"Henkka Seppälä": {"SS": ["bass"]},
		"Jaska Raatikainen": {"SS": ["drums"]}
	}}
      }' > /dev/null

aws --endpoint-url http://localhost:4566 dynamodb put-item \
    --table-name MusicCollection \
    --item '{
        "Artist": {"S": "Children of Bodom"},
        "AlbumTitle": {"S": "Hatebreeder"},
        "Genre": {"S": "Melodic death metal"},
	"Year": {"N": "1999"},
	"Songs": {"L": [ {"S": "Warheart"}, {"S": "Silent night Bodom night"}, {"S": "Hatebreeder"} ]},
	"Personnel": {"M": { 
		"Alexi Laiho": {"SS": ["lead guitar", "vocals"]},
		"Alexander Kuoppala": {"SS": ["rhythm guitar"]},
		"Janne Wirman": {"SS": ["keyboards"]},
		"Henkka Seppälä": {"SS": ["bass"]},
		"Jaska Raatikainen": {"SS": ["drums"]}
	}}
      }' > /dev/null

echo "Done"

