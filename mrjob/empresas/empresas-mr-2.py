from mrjob.job import MRJob
from mrjob.step import MRStep


class MRAccionesSiempreSubiendoOEstables(MRJob):

    def mapper(self, _, line):
        (empresa, valor, _) = line.split(',')
        yield empresa, float(valor)

    def reducer(self, key, values):
        valid = True
        last = -10.0
        for v in values:
            if last > 0 and v < last:
                valid = False
                break
            last = v
        if valid:
            yield None, key


if __name__ == '__main__':
    MRAccionesSiempreSubiendoOEstables.run()
