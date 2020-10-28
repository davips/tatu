--  Copyright (c) 2020. Davi Pereira dos Santos
--      This file is part of the tatu project.
--      Please respect the license. Removing authorship by any means
--      (by code make up or closing the sources) or ignoring property rights
--      is a crime and is unethical regarding the effort and time spent here.
--      Relevant employers or funding agencies will be notified accordingly.
--
--      tatu is free software: you can redistribute it and/or modify
--      it under the terms of the GNU General Public License as published by
--      the Free Software Foundation, either version 3 of the License, or
--      (at your option) any later version.
--
--      tatu is distributed in the hope that it will be useful,
--      but WITHOUT ANY WARRANTY; without even the implied warranty of
--      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
--      GNU General Public License for more details.
--
--      You should have received a copy of the GNU General Public License
--      along with tatu.  If not, see <http://www.gnu.org/licenses/>.
--

create table if not exists content (
    id char(23) NOT NULL primary key,
    value LONGBLOB NOT NULL
);

create table if not exists config (
    id char(23) NOT NULL primary key,
    params text NOT NULL
);
CREATE INDEX idx1 ON config (params(255));
-- 04fmE1EWKIsDKlrhOf6vky3 is the uuid of the empty config {}
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
-- 3oawXk8ZTPtS5DBsghkFNnz is the uuid of NoOp() also known as identity matrix.
insert into step values (null, '3oawXk8ZTPtS5DBsghkFNnz', 'NoOp', 'akangatu.noop', '04fmE1EWKIsDKlrhOf6vky3', null);


create table if not exists data (
    n integer NOT NULL primary key auto_increment,
    id char(23) NOT NULL UNIQUE,
    step char(23) NOT NULL,
    inn char(23),
    stream boolean not null,
    parent char(23) not null,
    locked boolean,
    unique(step, parent),
    FOREIGN KEY (step) REFERENCES step(id),
    FOREIGN KEY (inn) REFERENCES data(id),
    FOREIGN KEY (parent) REFERENCES data(id)
);
CREATE INDEX idx5 ON data (id);
CREATE INDEX idx6 ON data (step);
CREATE INDEX idx7 ON data (parent);
-- '3oawXk8ZTPtS5DBsghkFNnz' is the identity matrix / identity step: x(x) = x
insert into data values (null, '00000000000000000000001', '3oawXk8ZTPtS5DBsghkFNnz', null, 0, '00000000000000000000001', 0, now());

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
