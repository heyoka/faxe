from faxe import Faxe, Point, Batch


class Path(Faxe):

    @staticmethod
    def options():
        opts = [
            ("field", "string")
        ]
        return opts

    def init(self, args=None):
        self.path = args["field"]

    def handle_batch(self, batch_data):


    def handle_point(self, point_data):
        # val = Point.value(point_data, self.path)
        # print(val)
        # p = Point.new(Faxe.now())
        # Point.value(p, 'python.value', val)
        Point.default(point_data, 'pyth.field2', 'did not have field2')
        Point.default(point_data, 'pyth.field22', 'did not have field22')
        self.emit(point_data)


