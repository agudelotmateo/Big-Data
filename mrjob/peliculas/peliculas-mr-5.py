from mrjob.job import MRJob
from mrjob.step import MRStep


class MRDiaPeorEvaluacionPromedio(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer_aggregate),
            MRStep(reducer=self.reducer_min)
        ]

    def mapper(self, _, line):
        (_, _, _, rating, date) = line.split(',')
        yield date, float(rating)

    def reducer_aggregate(self, key, values):
        accum = 0
        length = 0
        for v in values:
            accum += v
            length += 1
        yield None, (key, accum/length)

    def reducer_min(self, _, values):
        min_value = 1e20
        date = ''
        for v in values:
            if v[1] < min_value:
                min_value = v[1]
                date = v[0]
        yield None, date


if __name__ == '__main__':
    MRDiaPeorEvaluacionPromedio.run()
