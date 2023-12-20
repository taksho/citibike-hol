/*******************************************************************************
    4. データ読み込みの準備
*******************************************************************************/

-- コマンドで操作する場合は以下を順に実行

-- use role sysadmin;
create database citibike;

use database citibike;
use schema public;
use warehouse compute_wh;

-- Trips テーブルを作成

create or replace table trips
(tripduration integer,
starttime timestamp,
stoptime timestamp,
start_station_id integer,
start_station_name string,
start_station_latitude float,
start_station_longitude float,
end_station_id integer,
end_station_name string,
end_station_latitude float,
end_station_longitude float,
bikeid integer,
membership_type string,
usertype string,
birth_year integer,
gender integer);

-- 外部ステージ作成 -> ただし、UI から作成済みの場合は実行しないで次のコマンドへ

create or replace stage citibike_trips
    url = 's3://snowflake-workshop-lab/citibike-trips-csv/'
;

-- 外部ステージ ファイルリスト確認

list @citibike_trips;

-- File Format 作成

create or replace file format csv type='csv'
  compression = 'auto' field_delimiter = ',' record_delimiter = '\n'
  skip_header = 0 field_optionally_enclosed_by = '\042' trim_space = false
  error_on_column_count_mismatch = false escape = 'none' escape_unenclosed_field = '\134'
  date_format = 'auto' timestamp_format = 'auto' null_if = ('') comment = 'file format for ingesting data for zero to snowflake';

-- 作成した File Format を確認

show file formats in database citibike;


/*******************************************************************************
    5.データの読み込み
*******************************************************************************/

-- 外部ステージからのデータロード

copy into trips from @citibike_trips file_format=csv PATTERN = '.*csv.*' ;

-- Trips テーブルにロードした全てのデータ、メタデータを削除

truncate table trips;

-- Trips テーブルの内容確認

select * from trips limit 10;

-- ウェアハウスサイズをコマンドで変更する場合は以下

alter warehouse compute_wh set warehouse_size='large';

-- ウェアハウスの確認

show warehouses;

-- 外部ステージからのデータロード

copy into trips from @citibike_trips file_format=csv PATTERN = '.*csv.*' ;


/*******************************************************************************
    6. クエリ、リザルトキャッシュ、およびクローンの操作
*******************************************************************************/

-- Trips テーブルの内容確認

select * from trips limit 20;

-- Citibike の使用状況に関する1時間ごとの基本統計量を確認
-- 確認内容：移動回数、平均移動時間、平均移動距離

select date_trunc('hour', starttime) as "date",
count(*) as "num trips",
avg(tripduration)/60 as "avg duration (mins)",
avg(haversine(start_station_latitude, start_station_longitude, end_station_latitude, end_station_longitude)) as "avg distance (km)"
from trips
group by 1 order by 1;

-- リザルトキャッシュの利用テスト（同じクエリを実行）

select date_trunc('hour', starttime) as "date",
count(*) as "num trips",
avg(tripduration)/60 as "avg duration (mins)",
avg(haversine(start_station_latitude, start_station_longitude, end_station_latitude, end_station_longitude)) as "avg distance (km)"
from trips
group by 1 order by 1;

-- Trips テーブルのゼロコピークローン Trips_dev の作成

create table trips_dev clone trips;


/*******************************************************************************
    7. 半構造化データ、ビュー、結合の操作
*******************************************************************************/

-- Weather データベースの作成

create database weather;

-- USE コマンドでのコンテキスト設定

-- use role sysadmin;
use warehouse compute_wh;
use database weather;
use schema public;

-- JSON データロード用テーブルの作成 -> Variant 型

create table json_weather_data (v variant);

-- Wether データ用に外部ステージの作成

create stage nyc_weather
url = 's3://snowflake-workshop-lab/zero-weather-nyc';

-- 作成したステージのファイルリスト表示

list @nyc_weather;

-- 半構造化データ（JSON）のロード

copy into json_weather_data
from @nyc_weather 
    file_format = (type = json strip_outer_array = true);

-- ロードした JSON データの確認

select * from json_weather_data limit 10;

-- 半構造化データを取り扱いやすい形に構造化して利用するためのビュー作成

create or replace view json_weather_data_view as
select
    v:obsTime::timestamp as observation_time,
    v:station::string as station_id,
    v:name::string as city_name,
    v:country::string as country,
    v:latitude::float as city_lat,
    v:longitude::float as city_lon,
    v:weatherCondition::string as weather_conditions,
    v:coco::int as weather_conditions_code,
    v:temp::float as temp,
    v:prcp::float as rain,
    v:tsun::float as tsun,
    v:wdir::float as wind_dir,
    v:wspd::float as wind_speed,
    v:dwpt::float as dew_point,
    v:rhum::float as relative_humidity,
    v:pres::float as pressure
from
    json_weather_data
where
    station_id = '72502';

-- 作成したビューの確認

select * from json_weather_data_view
where date_trunc('month',observation_time) = '2018-01-01'
limit 20;

-- Trips データと Weather データの結合

select weather_conditions as conditions
        ,count(*) as num_trips
from citibike.public.trips
left outer join json_weather_data_view
on date_trunc('hour', observation_time) = date_trunc('hour', starttime)
where conditions is not null
group by 1 order by 2 desc;


/*******************************************************************************
    8. タイムトラベルの使用
*******************************************************************************/

-- テーブルのドロップ

drop table json_weather_data;

-- ドロップされているかテーブル確認 -> エラー発生が正常

select * from json_weather_data limit 10;

-- Undrop でテーブルを復元

undrop table json_weather_data;

-- テーブルが復元されているかを確認

select * from json_weather_data limit 10;

-- USE コマンドでのコンテキスト設定

-- use role sysadmin;
use warehouse compute_wh;
use database citibike;
use schema public;

-- 意図的に誤った Update 処理を実行（全てのステーション名を「oops」に変更）

update trips set start_station_name = 'oops';

-- Update 結果の確認（乗車回数の上位20駅を確認するクエリを実行）-> 結果が1行しか返ってこない

select
    start_station_name as "station"
    ,count(*) as "rides"
from trips
group by 1
order by 2 desc
limit 20;

-- Update 結果を戻すため、直近で実行された Update コマンドのクエリIDを検索し、変数 $QUERY_ID に格納

set query_id =
    (select query_id 
     from table(information_schema.query_history_by_session (result_limit=>5))
     where query_text like 'update%'
     order by start_time desc limit 1);

-- タイムトラベルを使用して、Update 前の状態でテーブルを再作成

create or replace table trips as
(select * from trips before (statement => $query_id));

-- 再作成したテーブルでステーション名が復元されているか確認

select
    start_station_name as "station"
    ,count(*) as "rides"
from trips
group by 1
order by 2 desc
limit 20;


/*******************************************************************************
    9. ロール、Accountadmin、およびアカウントの使用状況の操作
*******************************************************************************/

-- USE コマンドで Accountadmin を操作

use role accountadmin;

-- 新しいロール Junior_DBA を作成し、自分に割り当て

create role junior_dba;
grant role junior_dba to user tshoji;

-- 作成、割り当てした Junior_DBA へ変更 -> ウェアハウスやデータベースの使用権限を確認

use role junior_dba;

-- Accountadmin へ変更し、compute_wh の使用権限を Junior_DBA へ付与

use role accountadmin;
grant usage on warehouse compute_wh to role junior_dba;

-- Junior_DBA へ変更し、使用できるウェアハウスを確認

use role junior_dba;
use warehouse compute_wh;

-- Accountadmin へ変更し、CITIBIKE, Weather データベースの使用権限を Junior_DBA へ付与

use role accountadmin;
grant usage on database citibike to role junior_dba;
grant usage on database weather to role junior_dba;

-- Junior_DBA へ変更し、使用できるデータベースを確認

use role junior_dba;


/*******************************************************************************
    11. Snowflake 環境のリセット
*******************************************************************************/

-- Accountadmin を使用して、今回作成した全てのオブジェクトを削除

use role accountadmin;

drop share if exists zero_to_snowflake_shared_data;
-- 必要に応じて、"zero_to_snowflake-shared_data" を共有に使用した名前に置き換え
drop database if exists citibike;
drop database if exists weather;
drop warehouse if exists analytics_wh;
drop warehouse if exists compute_wh;
drop role if exists junior_dba;
