# AUTHEXAMPLE
A minimal application to replicate basic authentication with `postgre` database, using `bcrypt` for password hashing


## User Struct

```
// User struct for holding data
type User struct {
	USER_ID    int
	USERNAME   string
	FULL_NAME  string
	PASSWORD   string
	EMAIL      string
	CREATED_ON time.Time
	LAST_LOGIN time.Time
}
```

## Table Structure

```
CREATE TABLE users (
	user_id serial PRIMARY KEY,
	username VARCHAR ( 30 ) UNIQUE NOT NULL,
    full_name VARCHAR (25) NOT NULL,
	password VARCHAR ( 60 ) NOT NULL,
	email VARCHAR ( 255 ) UNIQUE NOT NULL,
	created_on TIMESTAMP NOT NULL,
    last_login TIMESTAMP 
);
```
Application database lives in a docker container, you can ignore below commands if you are running it locally
*- make sure you run init.sql & seed.sql*

To build the docker image:

```
docker build . -t authexample:1.0
```

To run the docker image:

```
docker run -d -p 5432:5432 -e POSTGRES_DB=apidb -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres --name authexample authexample:1.0
```

<br />

## Working

Application exposes `/users` as restricted & `/hello` as unrestricted endpoint

Following are the username & passwords for users from seed data

| **USERNAME**| **PASSWORD**|
|:-: |:-: |
| admin| admin|
| jimmy| user|

<br />

## CURLs

## Get Requests

Get All Users

```
curl --request GET \
  --url http://localhost:3000/users \
  --header 'Authorization: Basic YWRtaW46YWRtaW4=' \
  --cookie sessionID=1234
```

Get User by username

```curl --request GET \
  --url http://localhost:3000/users/johnny \
  --header 'Authorization: Basic YWRtaW46YWRtaW4=' \
  --cookie sessionID=1234
```

## Post Request

Add New User

```
curl --request POST \
  --url http://localhost:3000/users \
  --header 'Authorization: Basic YWRtaW46YWRtaW4=' \
  --header 'Content-Type: application/json' \
  --cookie sessionID=1234 \
  --data '{
	"USER_ID": 3,
	"USERNAME": "johnny",
	"FULL_NAME": "Johnny Bravo",
	"PASSWORD": "johnny",
	"EMAIL": "johnny@example.com"
}'
```

## Delete Request

Delete User by username

```
curl --request DELETE \
  --url http://localhost:3000/users/johnny \
  --header 'Authorization: Basic YWRtaW46YWRtaW4=' \
  --cookie sessionID=1234
```

## Unrestricted Endpoint

```
curl --request GET \
  --url http://localhost:3000/hello \
  --cookie sessionID=1234
```

<br />

### Feel free to edit/expand/explore this repository

For feedback and queries, reach me on LinkedIn at [here](https://www.linkedin.com/in/usama28232/?original_referer=)