create table if not exists content (
    id char(23) NOT NULL primary key,
    value LONGBLOB NOT NULL
);
CREATE INDEX idx0 ON content (id);

create table if not exists config (
    id char(23) NOT NULL primary key,
    params text NOT NULL
);
CREATE INDEX idx1 ON config (params(255));
insert into config values ('04fmE1EWKIsDKlrhOf6vky3', '{}');

create table if not exists step (
    n integer NOT NULL primary key auto_increment,
    id char(23) NOT NULL UNIQUE,
    name char(60) NOT NULL,
    path varchar(250) NOT NULL,
    config char(23) NOT NULL,
    content char(23),
    FOREIGN KEY (config) REFERENCES config(id),
    FOREIGN KEY (content) REFERENCES content(id)
);
CREATE INDEX idx2 ON step (id);
CREATE INDEX idx3 ON step (name);
CREATE INDEX idx4 ON step (path);
-- 3oawXk8ZTPtS5DBsghkFNnz is the uuid of NoOp()
insert into step values (null, '048e4WDl9tFnovD6HYHePEb', 'NoOp', 'akangatu.noop', '04fmE1EWKIsDKlrhOf6vky3', null);


create table if not exists data (
    n integer NOT NULL primary key auto_increment,
    id char(23) NOT NULL UNIQUE,
    step char(23) NOT NULL,
    inn char(23),
    stream boolean,
    parent char(23) not null,
    locked boolean not null,
    t DATETIME,
    unique(step, parent),
    FOREIGN KEY (step) REFERENCES step(id),
    FOREIGN KEY (inn) REFERENCES data(id),
    FOREIGN KEY (parent) REFERENCES data(id)
);
CREATE INDEX idx5 ON data (id);
CREATE INDEX idx6 ON data (step);
CREATE INDEX idx7 ON data (parent);
-- '3oawXk8ZTPtS5DBsghkFNnz' is the identity matrix / identity step: x(x) = x
insert into data values (null, '00000000000000000000001', '048e4WDl9tFnovD6HYHePEb', null, false, '00000000000000000000001', false, now());

create table if not exists field (
    data char(23) NOT NULL,
    name char(24) NOT NULL,
    content char(23) NOT NULL,
    primary key (data, name),
    FOREIGN KEY (data) REFERENCES data(id),
    FOREIGN KEY (content) REFERENCES content(id)
);
CREATE INDEX idx8 ON field (name);
CREATE INDEX idx9 ON field (data);

create table if not exists storage (
    id char(23) NOT NULL primary key,
    data char(23) NOT NULL,
    t DATETIME,
    FOREIGN KEY (data) REFERENCES data(id)
);
CREATE INDEX idx10 ON storage (data);

create table if not exists stream (
    data char(23) NOT NULL,
    pos integer not null,
    chunk char(23) NOT NULL,
    primary key (data, pos),
    UNIQUE (data, chunk),
    FOREIGN KEY (data) REFERENCES data(id),
    FOREIGN KEY (chunk) REFERENCES data(id)
);
CREATE INDEX idx11 ON stream (chunk);

create table if not exists run (
    id char(23) NOT NULL primary key,
    data char(23) NOT NULL,
    duration float,
    node char(255) NOT NULL,
    alive DATETIME,
    t DATETIME,
    FOREIGN KEY (data) REFERENCES data(id)
);
CREATE INDEX idx12 ON run (data);
CREATE INDEX idx13 ON run (node);
