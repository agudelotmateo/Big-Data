movie = LOAD '/user/magude29/datasets/pig/peliculas.csv' USING PigStorage(',') as (user:int, movie:int, genre:chararray, rating:int, date:chararray);
group_movie = GROUP movie by movie;
usuarios_rating = FOREACH group_movie {
    unique_movie = DISTINCT movie.movie;
    GENERATE unique_movie, COUNT(movie.user), AVG(movie.rating);
}
DUMP usuarios_rating;
STORE usuarios_rating INTO '/user/magude29/datasets/pig/output/peliculas4' USING PigStorage(',');
