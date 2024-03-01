
--LINK

CREATE TABLE dvdrental.film_actor (
	gen_actor_id int8 NULL,
	gen_film_id int8 NULL,
	last_update timestamp NULL
);

CREATE TABLE dvdrental.film_category (
	gen_film_id int8 NULL,
	gen_category_id int8 NULL,
	last_update timestamp NULL
);

--Incremental

CREATE TABLE dvdrental.payment (
	gen_payment_id int8 NULL,
	source_payment_id int8 NULL,
	gen_customer_id int8 NULL,
	gen_staff_id int8 NULL,
	gen_rental_id int8 NULL,
	amount float8 NULL,
	payment_date timestamp NULL
);

CREATE TABLE dvdrental.rental (
	gen_rental_id int8 NULL,
	source_rental_id int8 NULL,
	rental_date timestamp NULL,
	gen_inventory_id int8 NULL,
	gen_customer_id int8 NULL,
	return_date timestamp NULL,
	gen_staff_id int8 NULL,
	last_update timestamp NULL
);

--SCDTYPE1

CREATE TABLE dvdrental.film (
	gen_film_id int8 NULL,
	source_film_id int8 NULL,
	title text NULL,
	description text NULL,
	release_year int8 NULL,
	gen_language_id int8 NULL,
	rental_duration int8 NULL,
	rental_rate float8 NULL,
	length int8 NULL,
	replacement_cost float8 NULL,
	rating text NULL,
	last_update timestamp NULL,
	special_features text NULL,
	fulltext text NULL
);

CREATE TABLE dvdrental.actor (
	gen_actor_id int8 NULL,
	source_actor_id int8 NULL,
	first_name text NULL,
	last_name text NULL,
	last_update timestamp NULL
);

CREATE TABLE dvdrental.city (
	gen_city_id int8 NULL,
	source_city_id int8 NULL,
	city text NULL,
	gen_country_id int8 NULL,
	last_update timestamp NULL
);

CREATE TABLE dvdrental.country (
	gen_country_id int8 NULL,
	source_country_id int8 NULL,
	country text NULL,
	last_update timestamp NULL
);

CREATE TABLE dvdrental."language" (
	gen_language_id int8 NULL,
	source_language_id int8 NULL,
	"name" text NULL,
	last_update timestamp NULL
);

CREATE TABLE dvdrental.category (
	gen_category_id int8 NULL,
	source_category_id int8 NULL,
	"name" text NULL,
	last_update timestamp NULL
);