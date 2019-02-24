create extension postgis;

--------------------------------------------------------------------------------
-- drop table jefferson_park;

select 'Jefferson Park' as name,
	st_geomfromgeojson(
	'{
    "type": "Polygon",
    "coordinates": [[
      [-84.43993799493921,33.68459520645089],
      [-84.43646185205591,33.682559712450825],
      [-84.43371527002466,33.68173835911907],
      [-84.43032495782984,33.680024205149735],
      [-84.42693464563501,33.68148838046054],
      [-84.41865198419703,33.681381246527216],
      [-84.41843740747584,33.69173690940296],
      [-84.42547552393091,33.69166549531089],
      [-84.43388693140162,33.68777333754376],
      [-84.43573229120386,33.68966587663938],
      [-84.43671934412134,33.68909454847353],
      [-84.43860761926783,33.68670207052906],
      [-84.43993799493921,33.68459520645089]
		]]
	}') as geom
into jefferson_park;

select name, geom, st_astext(geom) as word from jefferson_park;
--------------------------------------------------------------------------------
create schema raw;
alter schema raw owner to osaevtapyrcflq;

/*
drop table raw.east_point_incidents;
truncate table raw.east_point_incidents;
*/

create table if not exists raw.east_point_incidents
(
	incident_id integer,
	case_number text,
	incident_datetime text,
	incident_type_primary text,
	incident_description text,
	clearance_type text,
	address_1 text,
	address_2 text,
	city text,
	state text,
	zip text,
	country text,
	latitude numeric,
	longitude numeric,
	created_at text,
	updated_at text,
	location text,
	hour_of_day integer,
	day_of_week text,
	parent_incident_type text
);

alter table raw.east_point_incidents owner to osaevtapyrcflq;

select *
from raw.east_point_incidents;
--------------------------------------------------------------------------------
/*
drop table public.east_point_incidents;
truncate table public.east_point_incidents;
*/

create table if not exists public.east_point_incidents
(
	incident_id integer primary key,
	case_number text,
	incident_datetime timestamp,
	incident_type_primary text,
	incident_description text,
	clearance_type text,
	address_1 text,
	address_2 text,
	city text,
	state text,
	zip text,
	country text,
	latitude numeric,
	longitude numeric,
	created_at timestamp,
	updated_at timestamp,
	location geometry,
	hour_of_day integer,
	day_of_week text,
	parent_incident_type text,
	loaded_at timestamp default now()
);

alter table public.east_point_incidents owner to osaevtapyrcflq;

select *
from public.east_point_incidents;
--------------------------------------------------------------------------------
-- This is the merge statement from raw into public...
insert into public.east_point_incidents
  (incident_id, case_number, incident_datetime, incident_type_primary, incident_description, clearance_type,
   address_1, address_2, city, state, zip, country, latitude, longitude, created_at, updated_at, location,
   hour_of_day, day_of_week, parent_incident_type)
select
	incident_id, case_number, incident_datetime::timestamp, incident_type_primary, incident_description, clearance_type,
  address_1, address_2, city, state, zip, country, latitude, longitude, created_at::timestamp, updated_at::timestamp,
  st_makepoint(longitude, latitude) as location, hour_of_day, day_of_week, parent_incident_type
from raw.east_point_incidents
on conflict on constraint east_point_incidents_pkey
do
	update
		set case_number = excluded.case_number,
				incident_datetime = excluded.incident_datetime::timestamp,
				incident_type_primary = excluded.incident_type_primary,
				incident_description = excluded.incident_description,
				clearance_type = excluded.clearance_type,
				address_1 = excluded.address_1,
				address_2 = excluded.address_2,
				city = excluded.city,
				state = excluded.state,
				zip = excluded.zip,
				country = excluded.country,
				latitude = excluded.latitude,
				longitude = excluded.longitude,
				created_at = excluded.created_at::timestamp,
				updated_at = excluded.updated_at::timestamp,
				location = st_makepoint(excluded.longitude, excluded.latitude),
				hour_of_day = excluded.hour_of_day,
				day_of_week = excluded.day_of_week,
				parent_incident_type = excluded.parent_incident_type,
				loaded_at = now();
--------------------------------------------------------------------------------
-- This finds all incidents within Jefferson Park...
select a.*
from east_point_incidents as a
	join jefferson_park as b
		on st_within(a.location, b.geom)
order by a.incident_datetime desc
limit 25;

-- This does the same...like the one above better...
select b.*
from jefferson_park as a
	cross join east_point_incidents as b
where st_contains(a.geom, st_makepoint(b.longitude, b.latitude))
--------------------------------------------------------------------------------
