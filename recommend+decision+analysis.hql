########### Database Creating ################
## Data are uploaded from S3 buckets
## create database
create database california;
use california;

## Create tables and load data from S3 buckets
CREATE TABLE calendars
(listing_id BIGINT,
calendar_date string,
available string,
price STRING,
adjusted_price string,
minimum_nights int,
maximum_nights int
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
tblproperties ("skip.header.line.count"="1");
LOAD DATA INPATH 's3://california5/calendar.csv' INTO TABLE calendars;

CREATE TABLE reviews
(listing_id BIGINT,
id BIGINT,
review_date string,
reviewer_id BIGINT,
reviewer_name STRING,
comments string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
tblproperties ("skip.header.line.count"="1");
LOAD DATA INPATH 's3://california5/review_new.csv' INTO TABLE reviews;

CREATE TABLE host
(host_id BIGINT,
host_name string,
host_since string,
host_location string,
host_response_time string,
host_response_rate string,
host_acceptance_rate string,
host_is_superhost string,
host_neighbourhood string,
host_listings_count DOUBLE,
host_total_listings_count DOUBLE,
host_verifications string,
host_has_profile_pic string,
host_identity_verified string,
calculated_host_listings_count DOUBLE,
calculated_host_listings_count_entire_homes double,
calculated_host_listings_count_private_rooms double,
calculated_host_listings_count_shared_rooms double)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
tblproperties ("skip.header.line.count"="1");
LOAD DATA INPATH 's3://california5/host.csv' INTO TABLE host;

CREATE TABLE listing
(id BIGINT,
name string,
city string,
neighbourhood_cleansed string,
latitude DOUBLE,
longitude DOUBLE,
license string,
instant_bookable string,
host_id double,
property_type string,
room_type STRING,
accommodates DOUBLE,
bathrooms_text string,
bedrooms DOUBLE,
beds DOUBLE,
amenities string,
number_of_reviews double,
number_of_reviews_ltm double,
number_of_reviews_l30d double,
first_review string,
last_review string,
review_scores_rating DOUBLE,
review_scores_accuracy double,
review_scores_cleanliness DOUBLE,
review_scores_checkin double,
review_scores_communication double,
review_scores_location double,
review_scores_value double,
availability_30 double,
availability_60 double,
availability_90 double,
availability_365 double)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
tblproperties ("skip.header.line.count"="1");
LOAD DATA INPATH 's3://california5/listing.csv' INTO TABLE listing;


########### Decision-making benchmark ################
### table 1: filter the calendar data, get the ids and show their price and availability
DROP TABLE IF EXISTS t1;
CREATE TABLE t1 AS
SELECT c.listing_id, c.price, c.available FROM calendars c
WHERE c.listing_id IN 
(SELECT ls.id FROM listing ls 
WHERE ls.city ="los-angeles"
AND ls.neighbourhood_cleansed = "Hollywood"   
AND ls.room_type = 'Private room');

### table 2: average price, occupancy rate and revenue for each listing
DROP TABLE IF EXISTS t2;
CREATE TABLE t2 AS
SELECT listing_id, avg(cast(REPLACE (price,"$","") as double)) avg_price, sum(if(available = 'f',1,0))/count(*) as occupancy_rate,  avg(cast(REPLACE (price,"$","") as double)* if(available = 'f',1,0))) as revenue
FROM t1 
GROUP BY listing_id;

### table 3: percentile and grade of each listing
DROP TABLE IF EXISTS t3;
CREATE TABLE t3 AS 
SELECT *, CASE 
WHEN Percentile < 0.25 THEN 'D' 
WHEN Percentile < 0.5 THEN 'C'
WHEN Percentile < 0.75 THEN 'B'
WHEN Percentile <= 1 THEN 'A'
END AS grade FROM(
SELECT *, PERCENT_RANK() OVER(ORDER BY avg_price) AS percentile FROM t2) e;

## table 4: average price, occupancy rate and revenue of the filtered listings with selected grade
SELECT round(AVG(avg_price),2) as estimated_price,
       round(AVG(occupancy_rate),2) as estimated_occupany_rate,
       round(AVG(revenue),2) as estimated_revenue
 FROM 
(SELECT * FROM t3 WHERE grade = 'B') e3;   


########### RECOMMENDATION SYSTEM ################
# this query can recommend listings for the customer by provding a top N list of listing based on 
# customer's requirement on location, price range, date duration and accommodate
# and also the personalized rating based on their prioritized rating values

# parameter: neighbourhood, accommodate, start_date, end_date, min_price, max_price
use california;

# input parameter: neighbourhood & accommodate
# filter out listing by location and accommodate

### table 1: filter the city, neighbourhood and accommodate
DROP TABLE if exists listing_1;
Create table listing_1 as
select * from listing l 
where city = "los-angeles"
and neighbourhood_cleansed = "Hollywood" and accommodates=2;

### table 2: filter the expected duration date
DROP TABLE if exists listing_2;
create table listing_2 as
select *,REPLACE (adjusted_price,"$","") as price_int from calendars c
right join listing_1 l on l.id = c.listing_id
where c.calendar_date >= "2022-11-30" and c.calendar_date <= "2022-12-03";

### filter the price range and availability, order the list by the personalized rating
select listing_id
,avg(0.4*review_scores_accuracy+0.3*review_scores_cleanliness+0.2*review_scores_communication+0.1*review_scores_location) as rating 
from listing_2
group by listing_id
having sum(if(available="t", 1,0)) = (datediff("2023-12-03","2023-11-30")+1)
and avg(price_int)>=80 and avg(price_int)<=220
order by rating desc
limit 20;


########### Data Analysis ################
# revenue, occupancy rate, price by person for each city
### table 5: the listing situation
Create table t5 as
SELECT listing_id, avg(cast(REPLACE (price,"$","") as double)) avg_price, 
sum(if(available = 'f',1,0))/count(*) as occupancy_rate,  
sum(cast(REPLACE (price,"$","") as double)* if(available = 'f',1,0)) as revenue
FROM calendars
group by listing_id;

### get revenue, occupancy rate, price by person for each city
select city, avg(t.avg_price/l.accommodates) as ppp, avg(t.occupancy_rate) as occupancy_rate, avg(t.revenue) as revenue from t5 t
left OUTER JOIN listing l on l.id=t.listing_id
where city is not null
group by l.city;

### Top N revenue by neighbor
select neighbourhood_cleansed, avg(t.revenue) as revenue from t5 t
left OUTER JOIN listing l on l.id=t.listing_id
where l.neighbourhood_cleansed is not null
group by l.neighbourhood_cleansed
order by revenue desc
limit 10;

### Top N price by neighbor
select neighbourhood_cleansed, avg(t.avg_price/l.accommodates) as ppp from t5 t
left OUTER JOIN listing l on l.id=t.listing_id
where l.neighbourhood_cleansed is not null
group by l.neighbourhood_cleansed
order by ppp desc
limit 10;

### Top N revenue by host
select l.host_id, avg(t.revenue) as revenue from t5 t
left OUTER JOIN listing l on l.id=t.listing_id
where l.host_id is not null
group by l.host_id
order by revenue desc
limit 10;

# review score for superhost
select avg(l.review_scores_accuracy) as accuracy, avg(l.review_scores_cleanliness) as cleanliness, 
avg(l.review_scores_checkin) as checkin,avg(l. review_scores_communication) as communication,
avg(l. review_scores_location) as location, avg(l.review_scores_value) as value
from listing l
left OUTER JOIN host h on h.host_id=l.host_id
where h.host_is_superhost = 't';

# review score for non-superhost
select avg(l.review_scores_accuracy) as accuracy, avg(l.review_scores_cleanliness) as cleanliness, 
avg(l.review_scores_checkin) as checkin,avg(l. review_scores_communication) as communication,
avg(l. review_scores_location) as location, avg(l.review_scores_value) as value
from listing l
left OUTER JOIN host h on h.host_id=l.host_id
where h.host_is_superhost = 'f';

