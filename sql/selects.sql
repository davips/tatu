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

select step,inn,stream,parent,locked,name,content from data d inner join field f on d.id=f.data where d.id='00000000000000000000012';

select step,inn,stream,parent,locked,name as field_name,content as field_id 
from data d left join field f on d.id=f.data where d.id='00000000000000000000012';
