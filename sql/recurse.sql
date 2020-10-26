insert into step values (null, '99145678901234567890123', 'name3', 'path1', '04fmE1EWKIsDKlrhOf6vky3', null);
insert into step values (null, '12345678901234567890123', 'name1', 'path1', '04fmE1EWKIsDKlrhOf6vky3', null);
insert into step values (null, '22345678901234567890123', 'name2', 'path1', '04fmE1EWKIsDKlrhOf6vky3', null);
insert into step values (null, '32345678901234567890123', 'name3', 'path1', '04fmE1EWKIsDKlrhOf6vky3', null);
insert into step values (null, '42345678901234567890123', 'name3', 'path1', '04fmE1EWKIsDKlrhOf6vky3', null);
insert into step values (null, '52345678901234567890123', 'name3', 'path1', '04fmE1EWKIsDKlrhOf6vky3', null);
insert into step values (null, '62345678901234567890123', 'name3', 'path1', '04fmE1EWKIsDKlrhOf6vky3', null);
insert into step values (null, '72345678901234567890123', 'name3', 'path1', '04fmE1EWKIsDKlrhOf6vky3', null);
insert into step values (null, '82345678901234567890123', 'name3', 'path1', '04fmE1EWKIsDKlrhOf6vky3', null);
insert into step values (null, '92345678901234567890123', 'name3', 'path1', '04fmE1EWKIsDKlrhOf6vky3', null);

insert into data values (null, '00000000000000000000091', '99145678901234567890123', null, 0, '00000000000000000000001', 0, now());

insert into data values (null, '00000000000000000000011', '12345678901234567890123', null, 0, '00000000000000000000091', 0, now());
insert into data values (null, '00000000000000000000012', '22345678901234567890123', null, 0, '00000000000000000000091', 0, now());

insert into data values (null, '00000000000000000000021', '42345678901234567890123', null, 0, '00000000000000000000012', 0, now());
insert into data values (null, '00000000000000000000022', '52345678901234567890123', null, 0, '00000000000000000000012', 0, now());

insert into data values (null, '00000000000000000000013', '32345678901234567890123', null, 0, '00000000000000000000021', 0, now());

insert into data values (null, '00000000000000000000031', '62345678901234567890123', null, 0, '00000000000000000000013', 0, now());
insert into data values (null, '00000000000000000000032', '72345678901234567890123', null, 0, '00000000000000000000013', 0, now());
insert into data values (null, '00000000000000000000033', '82345678901234567890123', null, 0, '00000000000000000000013', 0, now());

insert into data values (null, '00000000000000000000099', '92345678901234567890123', null, 0, '00000000000000000000022', 0, now());

select * from step;

select * from data;

-- self + ancestors
WITH RECURSIVE cnt(id, parent) AS (VALUES('00000000000000000000012', '00000000000000000000091') UNION ALL 
select d.id, d.parent FROM cnt inner join data d ON cnt.parent=d.id and d.id<>d.parent)
select id, parent FROM cnt;

-- self + children
WITH RECURSIVE cnt(id, parent) AS (VALUES('00000000000000000000012', '00000000000000000000091') UNION ALL 
select d.id, d.parent FROM cnt inner join data d ON d.parent=cnt.id and d.id<>d.parent)
select id, parent FROM cnt;

--1 ={
--  91: {
--        11: {},
--        12: {
--              21: {
--                    13: {
--                          31: {},
--                          32: {},
--                          33: {},
--                        }
--                  },
--              22: {
--                    99: {}
--                  },
--            },
--      }
--   }

