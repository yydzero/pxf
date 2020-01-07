-- @description query01 for Parquet pushdown

-- no filter
select * from parquet_types_hcfs_r;

-- filter by integer
select * from parquet_types_hcfs_r where num1 = 11;

select * from parquet_types_hcfs_r where num1 < 11;

select * from parquet_types_hcfs_r where num1 > 11;

select * from parquet_types_hcfs_r where num1 <= 11;

select * from parquet_types_hcfs_r where num1 >= 11;

select * from parquet_types_hcfs_r where num1 <> 11;

select * from parquet_types_hcfs_r where num1 is null;

select * from parquet_types_hcfs_r where num1 is not null;

-- filter by bigint
select * from parquet_types_hcfs_r where bg = 2147483655;

select * from parquet_types_hcfs_r where bg < 0;

select * from parquet_types_hcfs_r where bg > 2147483655;

select * from parquet_types_hcfs_r where bg <= -2147483643;

select * from parquet_types_hcfs_r where bg >= 2147483655;

select * from parquet_types_hcfs_r where bg <> -1;

select * from parquet_types_hcfs_r where bg is null;

select * from parquet_types_hcfs_r where bg is not null;

-- filter by real
select * from parquet_types_hcfs_r where r = 8.7;

select * from parquet_types_hcfs_r where r < 8.7;

select * from parquet_types_hcfs_r where r > 8.7;

select * from parquet_types_hcfs_r where r <= 8.7;

select * from parquet_types_hcfs_r where r >= 8.7;

select * from parquet_types_hcfs_r where r <> 8.7;

select * from parquet_types_hcfs_r where r is null;

select * from parquet_types_hcfs_r where r is not null;

-- filter by text
select * from parquet_types_hcfs_r where grade = 'excellent';

select * from parquet_types_hcfs_r where grade < 'excellent';

select * from parquet_types_hcfs_r where grade > 'excellent';

select * from parquet_types_hcfs_r where grade <= 'excellent';

select * from parquet_types_hcfs_r where grade >= 'excellent';

select * from parquet_types_hcfs_r where grade <> 'excellent';

select * from parquet_types_hcfs_r where grade is null;

select * from parquet_types_hcfs_r where grade is not null;

-- filter by varchar
select * from parquet_types_hcfs_r where vc1 = 's_16';

select * from parquet_types_hcfs_r where vc1 < 's_10';

select * from parquet_types_hcfs_r where vc1 > 's_168';

select * from parquet_types_hcfs_r where vc1 <= 's_10';

select * from parquet_types_hcfs_r where vc1 >= 's_168';

select * from parquet_types_hcfs_r where vc1 <> 's_16';

select * from parquet_types_hcfs_r where vc1 IS NULL;

select * from parquet_types_hcfs_r where vc1 IS NOT NULL;

-- filter by char
select * from parquet_types_hcfs_r where c1 = 'EUR';

select * from parquet_types_hcfs_r where c1 < 'USD';

select * from parquet_types_hcfs_r where c1 > 'EUR';

select * from parquet_types_hcfs_r where c1 <= 'EUR';

select * from parquet_types_hcfs_r where c1 >= 'USD';

select * from parquet_types_hcfs_r where c1 <> 'USD';

select * from parquet_types_hcfs_r where c1 IS NULL;

select * from parquet_types_hcfs_r where c1 IS NOT NULL;

-- filter by smallint
select * from parquet_types_hcfs_r where sml = 1000;

select * from parquet_types_hcfs_r where sml < -1000;

select * from parquet_types_hcfs_r where sml > 31000;

select * from parquet_types_hcfs_r where sml <= 0;

select * from parquet_types_hcfs_r where sml >= 0;

select * from parquet_types_hcfs_r where sml <> 0;

select * from parquet_types_hcfs_r where sml IS NULL;

select * from parquet_types_hcfs_r where sml IS NOT NULL;

-- filter by date
select * from parquet_types_hcfs_r where cdate = '2019-12-04';

select * from parquet_types_hcfs_r where cdate < '2019-12-04';

select * from parquet_types_hcfs_r where cdate > '2019-12-20';

select * from parquet_types_hcfs_r where cdate <= '2019-12-06';

select * from parquet_types_hcfs_r where cdate >= '2019-12-15';

select * from parquet_types_hcfs_r where cdate <> '2019-12-15';

select * from parquet_types_hcfs_r where cdate IS NULL;

select * from parquet_types_hcfs_r where cdate IS NOT NULL;

-- filter by float8
select * from parquet_types_hcfs_r where amt = 1200;

select * from parquet_types_hcfs_r where amt < 1500;

select * from parquet_types_hcfs_r where amt > 2500;

select * from parquet_types_hcfs_r where amt <= 1500;

select * from parquet_types_hcfs_r where amt >= 2550;

select * from parquet_types_hcfs_r where amt <> 1200;

select * from parquet_types_hcfs_r where amt IS NULL;

select * from parquet_types_hcfs_r where amt IS NOT NULL;

-- filter by bytea
select * from parquet_types_hcfs_r where bin = '1';

select * from parquet_types_hcfs_r where bin < '1';

select * from parquet_types_hcfs_r where bin > '1';

select * from parquet_types_hcfs_r where bin <= '1';

select * from parquet_types_hcfs_r where bin >= '1';

select * from parquet_types_hcfs_r where bin <> '1';

select * from parquet_types_hcfs_r where bin IS NULL;

select * from parquet_types_hcfs_r where bin IS NOT NULL;

-- filter by boolean
select * from parquet_types_hcfs_r where b;

select * from parquet_types_hcfs_r where b == true;

select * from parquet_types_hcfs_r where b <> true;

select * from parquet_types_hcfs_r where b == false;

select * from parquet_types_hcfs_r where b <> false;

select * from parquet_types_hcfs_r where not b;

-- filter by id column with projection
select id from parquet_types_hcfs_r where id = 5;

select name, cdate, amt, sml, num1 from parquet_types_hcfs_r where id = 8 or (id > 10 and grade = 'bad');

select bin, bg, tm from parquet_types_hcfs_r where id = 15;

-- filter by date and amt
select * from parquet_types_hcfs_r where cdate > '2019-12-02' and cdate < '2019-12-12' and amt > 1500;

-- filter by date with or and amt
select * from parquet_types_hcfs_r where cdate > '2019-12-19' OR ( cdate <= '2019-12-15' and amt > 2000);

-- filter by date with or and amt using column projection
select id, amt, b where cdate > '2019-12-19' OR ( cdate <= '2019-12-15' and amt > 2000);

-- filter by date or amt
select * from parquet_types_hcfs_r where cdate > '2019-12-20' OR amt < 1500;

-- filter by timestamp (not pushed)
select * from parquet_types_hcfs_r where tm = '2013-07-23 21:00:00';

-- filter by decimal (not pushed)
select * from parquet_types_hcfs_r where dec2 = 0;

-- filter by in (not pushed)
select * from parquet_types_hcfs_r where num1 in (11, 12);