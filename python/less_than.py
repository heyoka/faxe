from faxe import Faxe
from faxe import Point
from faxe import Batch


class Less_than(Faxe):
    """
    simple less-than batch filter node using the 'Batch' and 'Point' helper classes

    Filters out points, that have fields (.fields() option) with values below given thresholds (.values() option)
    Only numerical values are considered

    use in dfs:

    @less_than()
    .fields(    'field1',   'field2',   'field3')
    .values(    14,         3.048,      7)

    """

    @staticmethod
    def options():
        opts = [
            ('fields', 'string_list'),
            ('values', 'number_list')
        ]
        return opts

    def init(self, args=None):
        self.fields = list(args['fields'])
        self.values = list(args['values'])

    def handle_point(self, point_data):
        self.emit(point_data)

    def handle_batch(self, batch_data):
        """
        uses the built-in 'filter' function to filter the list of points, utilizing the 'filter_fun' method below
        :param batch_data:
        :return:
        """
        # filter points from the data-batch
        filtered_points = filter(self.filter_fun, Batch.points(batch_data))
        # set the filtered list of points back to the batch
        Batch.points(batch_data, list(filtered_points))
        # emit the filtered batch
        self.emit(batch_data)

    def filter_fun(self, point):
        """
        for a single DataPoint iterate over the fields given with options
        and compare values to thresholds
        :param point:
        :return:
        """
        for idx, fieldname in enumerate(self.fields):
            if Point.field(point, fieldname):
                val = Point.field(point, fieldname)
                if (type(val) == int or type(val) == float) and val >= self.values[idx]:
                    return False

        return True

