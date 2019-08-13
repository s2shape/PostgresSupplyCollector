create table test_data_types (
   serial_field serial PRIMARY KEY,
   bool_field boolean,
   char_field char(40),
   varchar_field varchar(100),
   text_field text,
   smallint_field smallint,
   int_field integer,
   float_field float,
   real_field real,
   numeric_field numeric,
   date_field date,
   time_field time,
   timestamp_field timestamp,
   timestamptz_field timestamptz,
   interval_field interval,
   json_field json,
   uuid_field uuid   
);

insert into test_data_types(bool_field, char_field, varchar_field, text_field, smallint_field, int_field, float_field, real_field, numeric_field, date_field, time_field, timestamp_field, timestamptz_field, interval_field, json_field, uuid_field)
values(True, 'char!', 'varchar!', 'text!', 1, 299792458, 6.626, 1.280649, 6.02214076, '2019-08-13', '17:40:00', '2019-08-13 17:40:00-07', '2019-08-13 17:40:00-07', '1 hour ago', '{"field":"value"}', 'a81bc81b-dead-4e5d-abff-90865d1e13b1');

create table test_arrays (
   id serial PRIMARY KEY,
   text_array text[],
   int_array integer[]   
);

insert into test_arrays(text_array, int_array)
values(ARRAY['one', 'two'], ARRAY[1,2,3])

create table test_field_names (
   id serial PRIMARY KEY,
   low_case integer,
   UPCASE integer,
   CamelCase integer,
   "Table" integer,
   "array" integer,
   "SELECT" integer
);

insert into test_field_names(low_case, upcase, camelcase, "Table", "array", "SELECT")
values(0,0,0,0,0,0);

create table test_index (
   id serial NOT NULL PRIMARY KEY,
   name varchar(100) NOT NULL UNIQUE
);

insert into test_index(name)
values('Sunday');
insert into test_index(name)
values('Monday');
insert into test_index(name)
values('Tuesday');
insert into test_index(name)
values('Wednesday');
insert into test_index(name)
values('Thursday');
insert into test_index(name)
values('Friday');
insert into test_index(name)
values('Saturday');

create table test_index_ref (
   id serial PRIMARY KEY,
   index_id integer REFERENCES test_index(id)
);

insert into test_index_ref(index_id)
values(1);
insert into test_index_ref(index_id)
values(5);
