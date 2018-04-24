from mrjob.job import MRJob


class MRNumUsuariosYRatingPromedioPorPelicula(MRJob):

    def mapper(self, _, line):
        (_, movie_id, _, rating, _) = line.split(',')
        yield movie_id, (1, float(rating))

    def reducer(self, key, values):
        length = 0
        rating = 0
        for v in values:
            rating += v[1]
            length += 1
        yield key, (length, rating/length)


if __name__ == '__main__':
    MRNumUsuariosYRatingPromedioPorPelicula.run()
