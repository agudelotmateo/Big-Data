movie = LOAD '/user/magude29/datasets/pig/peliculas.csv' USING PigStorage(',') as (user:int, movie:int, genre:chararray, rating:int, date:chararray);
group_date = GROUP movie BY date;
movie_ctr = FOREACH group_date GENERATE group, COUNT(movie.movie) AS count;
group_all = GROUP movie_ctr ALL;
max_day = FOREACH group_all GENERATE movie_ctr, MIN(movie_ctr.count);
STORE max_day INTO '/user/magude29/datasets/pig/output/peliculas3' USING PigStorage(',');
