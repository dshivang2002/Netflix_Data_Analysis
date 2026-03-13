select * from netflix_data 
where show_id = 's5319' or show_id = 's5752'

-- Cleaning of the data using Sql 

-- Remove duplicates 

select show_id , count(1)
from netflix_data 
group by 1 
having count(1) > 1 

select * 
from netflix_data 
where concat(upper(title),type) in ( select concat(upper(title) , type)
from netflix_data 
group by upper(title) , type
having count(*) > 1
)
order by title


-- it returns the unique data 
select * from (
select *, 
Row_Number() over( partition by upper(title) , type order by show_id ) as rn 
from netflix_data
) 
where rn = 1

-- new tables for directors , listed_in , cast , country 


-- creating new table netflix_listed_in 

SELECT show_id,
       trim(unnest(string_to_array(listed_in, ','))) AS listed_in
into netflix_listed_in 
FROM netflix_data;

select * from netflix_listed_in

-- creating new table netflix_directors 

SELECT show_id,
       trim(unnest(string_to_array(director, ','))) AS director_name
into netflix_directors 
FROM netflix_data;

select * from netflix_listed_in
-- creating new table netflix_cast

SELECT show_id,
       trim(actor) AS actor
INTO netflix_cast
FROM netflix_data
CROSS JOIN LATERAL unnest(string_to_array("cast", ',')) AS actor;


select * from netflix_cast


-- creating new table netflix_country

SELECT show_id,
       trim(country_name) AS country
INTO netflix_country
FROM netflix_data
CROSS JOIN LATERAL unnest(string_to_array("country", ',')) AS country_name;

alter table netflix_listed_in rename to netflix_genre



-- Deal with missing values 
insert into netflix_country
select show_id ,m.country
from netflix_data as nr
inner join (select director,country
from netflix_data as nd
inner join netflix_directors as n
on nd.show_id = n.show_id 
group by 1,2
having country is not null
order by 1) as m 
on nr.director = m.director
where nr.country is null
order by 1

select director,country
from netflix_data as nd
join netflix_directors as n
on nd.show_id = n.show_id 
group by director,country
having country is not null
order by director

-- 
select * from netflix_data
case when duration is null then rating else duration end 
where duration is null 



-- Final table After Cleaning

with cte as (select *, 
Row_Number() over( partition by upper(title) , type order by show_id ) as rn 
from netflix_data
) 

select show_id ,type,title,date_added,release_year,rating,
case when duration is null then rating else duration end as duration , description 
into netflix_final
from cte 
where rn = 1 
order by show_id



-- Analysis 

select * from netflix_directors

--netflix data analysis
/*1 for each director count the no of movies and tv shows created by them in separate columns for directors
    who have created tv shows and movies both */

select nd.director_name,
count(distinct case when n.type ='Movies' then n.show_id end ) as no_of_movies ,
count(distinct case when n.type ='TV Show' then n.show_id end ) as no_of_TVshow
from netflix_final as n
join netflix_directors as nd 
on n.show_id = nd.show_id
group by nd.director_name
having count(distinct n.type) > 1
order by 1


-- 2. which country has highest no. of comedy movies
TV Horror 
Comedies

select nc.country ,count(distinct nf.show_id) as no_of_movies
from netflix_final as nf
join netflix_country as nc on nf.show_id = nc.show_id
join netflix_genre as ng on nf.show_id = ng.show_id
where ng.listed_in = 'Comedies' and nf.type = 'Movie'
group by 1
order by 2 desc
limit 1 

--3 For each year which director has max number of movie released 
with cte as (
SELECT nd.director_name ,EXTRACT(YEAR FROM TO_DATE(date_added,'Month DD, YYYY')) AS year_added ,
count(nf.show_id) as no_of_movies
FROM netflix_final as nf
join netflix_directors as nd on nf.show_id = nd.show_id
where nf.type = 'Movie'
group by 1,2
), cte2  as (
select *,
Row_Number() over(partition by year_added order by no_of_movies desc , director_name) as rn 
from cte
-- order by year_added,no_of_movies desc
) 

select * 
from cte2 
where rn = 1


--  4. what is the average duration of movie in each genre 
with cte as (
select ng.listed_in as genre, trim(replace(nf.duration,'min','')):: numeric as duration_int
from netflix_final as nf 
join netflix_genre as ng 
on nf.show_id = ng.show_id 
where nf.type = 'Movie'
)

select genre ,round(avg(duration_int),2) as average_duration
from cte 
group by 1
order by 1


-- 5. Find the list of directors who have directed both comedy and horror movies both 
-- display director name along with the no of movie and TV show 

select nd.director_name,
count (distinct case when ng.listed_in= 'Comedies' then ng.show_id end) as no_of_comedy,
count (distinct case when ng.listed_in = 'Horror Movies' then ng.show_id end)
from netflix_final as nf 
join netflix_directors as nd on nf.show_id = nd.show_id 
join netflix_genre as ng on nf.show_id = ng.show_id 
where nf.type = 'Movie' and ng.listed_in in ('Horror Movies' , 'Comedies')
group by 1
having count(distinct ng.listed_in ) = 2




