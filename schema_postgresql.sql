CREATE TABLE public.products (
	id serial4 NOT NULL,
	"name" varchar NULL,
	price float4 NULL,
	rating float4 NULL,
	rating_count float4 NULL,
	"timestamp" timestamp NOT NULL
);

CREATE TABLE public."statistics" (
	id serial4 NOT NULL,
	db_size int4 NOT NULL,
	count_time float8 NULL,
	median_time float8 NULL,
	average_time float8 NULL,
	select_time float8 NULL,
	update_time float8 NULL,
	aggregation_time float8 NULL,
	sort_time float8 NULL,
	db_type varchar NULL
);