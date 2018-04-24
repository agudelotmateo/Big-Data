from mrjob.job import MRJob
from mrjob.step import MRStep


class MRDiaNegro(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_min,
                   reducer=self.reducer_min),
            MRStep(mapper=self.mapper_counter,
                   reducer=self.reducer_counter),
            MRStep(reducer=self.reducer_aggregate),
            MRStep(reducer=self.reducer_worst_date)
        ]

    def mapper_min(self, _, line):
        (empresa, valor, fecha) = line.split(',')
        yield empresa, (float(valor), fecha)

    def reducer_min(self, key, values):
        min_value = 1e20
        min_date = ''
        for v in values:
            if v[0] < min_value:
                min_value = v[0]
                min_date = v[1]
        yield None, min_date

    def mapper_counter(self, _, value):
        yield value, 1

    def reducer_counter(self, key, values):
        yield key, sum(values)

    def reducer_aggregate(self, key, values):
        for v in values:
            yield None, (key, v)

    def reducer_worst_date(self, _, values):
        max_value = -1
        worst_date = ''
        for v in values:
            # suposicion de inflacion: $x ahora valen menos que esos mismos $x antes
            if v[1] >= max_value:
                max_value = v[1]
                worst_date = v[0]
        yield None, worst_date

if __name__ == '__main__':
    MRDiaNegro.run()
