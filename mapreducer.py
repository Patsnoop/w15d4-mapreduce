from mrjob.job import MRJob
from mrjob.step import MRStep

class ProductCategoryCount(MRJob):

    def mapper(self, _, line):
        fields = line.split(',')
        product_category = fields[2]
        purchase_amount = int(fields[3])
        yield product_category, purchase_amount

    def combiner(self, product_category, counts):
        yield product_category, sum(counts)

    def reducer(self, product_category, counts):
        yield product_category, sum(counts)

if __name__ == '__main__':
    ProductCategoryCount.run()

class TopProducts(MRJob):

    def mapper(self, _, line):
        fields = line.split(',')
        product_id = fields[1]
        purchase_amount = int(fields[3])
        yield product_id, purchase_amount

    def combiner(self, product_id, counts):
        yield product_id, sum(counts)

    def reducer(self, product_id, counts):
        yield None, (sum(counts), product_id)

    def reducer_find_top_5(self, _, product_counts):
        top_5 = sorted(product_counts, reverse=True)[:5]
        for count, product_id in top_5:
            yield product_id, count

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   combiner=self.combiner,
                   reducer=self.reducer),
            MRStep(reducer=self.reducer_find_top_5)
        ]

if __name__ == '__main__':
    TopProducts.run()



class AverageProductsPerOrder(MRJob):

    def mapper(self, _, line):
        fields = line.split(',')
        order_id = fields[0]
        purchase_amount = int(fields[3])
        yield order_id, purchase_amount

    def reducer(self, order_id, counts):
        yield None, sum(counts)

    def reducer_calculate_average(self, _, total_counts):
        total_orders = 0
        total_products = 0
        for count in total_counts:
            total_orders += 1
            total_products += count
        yield 'Average Products Per Order', total_products / total_orders

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(reducer=self.reducer_calculate_average)
        ]

if __name__ == '__main__':
    AverageProductsPerOrder.run()