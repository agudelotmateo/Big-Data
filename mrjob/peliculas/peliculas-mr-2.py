from mrjob.job import MRJob
from mrjob.step import MRStep


class MRDiaMasPeliculasVistas(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer_aggregate),
            MRStep(reducer=self.reducer_max)
        ]

    def mapper(self, _, line):
        (_, _, _, _, date) = line.split(',')
        yield date, 1

    def reducer_aggregate(self, key, values):
        yield None, (key, sum(values))

    def reducer_max(self, _, values):
        max_value = -1
        date = ''
        for v in values:
            if v[1] > max_value:
                max_value = v[1]
                date = v[0]
        yield None, date


if __name__ == '__main__':
    MRDiaMasPeliculasVistas.run()
