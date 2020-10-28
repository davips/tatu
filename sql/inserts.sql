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

insert into content values('0000000000000000000021X', 'blob 21 x');
insert into field values('00000000000000000000012', 'X', '0000000000000000000021X');

insert into content values('0000000000000000000021Y', 'blob 21 y');
insert into field values('00000000000000000000012', 'Y', '0000000000000000000021Y');


insert into data values (null, '00000000000000000000199', '92345678901234567890123', null, 1, '00000000000000000000013', 0, now());
insert into stream values('00000000000000000000199', 0, '00000000000000000000031');
insert into stream values('00000000000000000000199', 1, '00000000000000000000032');
insert into stream values('00000000000000000000199', 2, '00000000000000000000033');
