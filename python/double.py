from faxe import Faxe, Point, Batch


class Double(Faxe):

    @staticmethod
    def options():
        opts = [
            ("field", "string"),
            ("as", "string")
        ]
        return opts

    def init(self, args=None):
        self.fieldname = args["field"]
        self.asfieldname = args["as"]

    def handle_point(self, point_data):
        self.emit(self.calc(point_data))

    def handle_batch(self, batch_data):
        for point in Batch.points(batch_data):
            self.calc(point)
        self.emit(batch_data)

    def calc(self, point_dict):
        Point.value(point_dict, self.asfieldname, (Point.value(point_dict, self.fieldname) * 2))
        return point_dict

