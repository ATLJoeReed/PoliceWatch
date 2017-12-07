drop table east_point_police_department_20171206;

select count(*)
from east_point_police_department_20171206;

select *
from east_point_police_department_20171206
order by incident_datetime::timestamp desc;

select
  case_number,
  incident_id::int as incident_id,
  incident_datetime::timestamp as incident_datetime,
  incident_type_primary,
  incident_description,
  clearance_type,
  address_1,
  address_2,
  city,
  state,
  zip,
  country,
  latitude,
  longitude,
  created_at::timestamp as created_at,
  updated_at::timestamp as updated_at,
  location,
  hour_of_day,
  day_of_week,
  parent_incident_type
into incidents
from east_point_police_department_20171206;


select max(incident_datetime)
from incidents;

create extension cube;
create extension earthdistance;

-- East Point MARTA Station
-- lat:   33.676963
-- long: -84.440734

SELECT
  cast(
      earth_distance(
          ll_to_earth(33.676963, -84.440734),
          ll_to_earth(latitude, longitude)
      ) * .0006213712 AS NUMERIC(10, 2)
  ) AS distance,
  incident_type_primary,
  incident_description,
  parent_incident_type,
  address_1,
  city,
  state,
  zip
FROM incidents
ORDER BY distance desc
LIMIT 50;

