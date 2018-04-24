from mrjob.job import MRJob
from mrjob.step import MRStep


class MRDiaMenosPeliculasVistas(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer_aggregate),
            MRStep(reducer=self.reducer_min)
        ]

    def mapper(self, _, line):
        (_, _, _, _, date) = line.split(',')
        yield date, 1

    def reducer_aggregate(self, key, values):
        yield None, (key, sum(values))

    def reducer_min(self, _, values):
        min_value = 1e20
        date = ''
        for v in values:
            if v[1] < min_value:
                min_value = v[1]
                date = v[0]
        yield None, date


if __name__ == '__main__':
    MRDiaMenosPeliculasVistas.run()
