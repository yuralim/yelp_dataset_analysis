DROP TABLE IF EXISTS public.business;
DROP TABLE IF EXISTS public.business_category;
DROP TABLE IF EXISTS public.review;
DROP TABLE IF EXISTS public.review_text;
DROP TABLE IF EXISTS public.tip;
DROP TABLE IF EXISTS public.tip_text;
DROP TABLE IF EXISTS public.checkin;
DROP TABLE IF EXISTS public.yelp_user;
DROP TABLE IF EXISTS public.yelp_user_elite;
DROP TABLE IF EXISTS public.yelp_user_friend;
DROP TABLE IF EXISTS public.script_log;

CREATE TABLE public.business (
  business_id varchar(32) PRIMARY KEY,
  "address" varchar(256),
  city varchar(256),
  latitude double precision,
  longitude double precision,
  "name" varchar(256),
  postal_code varchar(256),
  "state" varchar(32)
);

CREATE TABLE public.business_category (
  business_id varchar(32) NOT NULL,
  category varchar(128)
)
diststyle key distkey (business_id);

CREATE TABLE public.review (
  review_id varchar(32) PRIMARY KEY,
  "user_id" varchar(32) NOT NULL,
  business_id varchar(32) NOT NULL,
  stars double precision,
  "date" timestamp,
  "text" varchar(max),
  sentiment varchar(8)
);

CREATE TABLE public.review_text (
  review_id varchar(32) NOT NULL,
  word varchar(max) 
)
diststyle key distkey (review_id);

CREATE TABLE public.tip (
  tip_id varchar(36) PRIMARY KEY,
  "user_id" varchar(32) NOT NULL,
  "date" timestamp,
  business_id varchar(32) NOT NULL,
  "text" varchar(max),
  sentiment varchar(8)
);

CREATE TABLE public.tip_text (
  tip_id varchar(36) NOT NULL,
  word varchar(max)
)
diststyle key distkey (tip_id);

CREATE TABLE public.checkin (
  business_id varchar(32) NOT NULL,
  "date" timestamp
)
diststyle key distkey (business_id);

CREATE TABLE public.yelp_user (
  "user_id" varchar(32) PRIMARY KEY,
  "name" varchar(256)
);

CREATE TABLE public.yelp_user_elite (
  "user_id" varchar(32) NOT NULL,
  "year" varchar(4) NOT NULL
)
diststyle key distkey (user_id);

CREATE TABLE public.yelp_user_friend (
  "user_id" varchar(32) NOT NULL,
  "friend_id" varchar(32) NOT NULL
)
diststyle key distkey (user_id);

CREATE TABLE public.script_log (
  script_name varchar(32) NOT NULL,
  processed_file smallint
)
diststyle key distkey (script_name);