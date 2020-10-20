select step,inn,stream,parent,locked,name,content from data d inner join field f on d.id=f.data where d.id='00000000000000000000012';

select step,inn,stream,parent,locked,name as field_name,content as field_id 
from data d left join field f on d.id=f.data where d.id='00000000000000000000012';
