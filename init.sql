CREATE TABLE users (
	user_id serial PRIMARY KEY,
	username VARCHAR ( 30 ) UNIQUE NOT NULL,
    full_name VARCHAR (25) NOT NULL,
	password VARCHAR ( 60 ) NOT NULL,
	email VARCHAR ( 255 ) UNIQUE NOT NULL,
	created_on TIMESTAMP NOT NULL,
    last_login TIMESTAMP 
);

