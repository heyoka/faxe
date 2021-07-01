from faxe import Faxe


class Double(Faxe):

    @staticmethod
    def options():
        opts = [
            (b'field', b'string'),
            (b'as', b'string')
        ]
        return opts

    def init(self, args=None):
        self.fieldname = args["field"]
        self.asfieldname = args["as"]
        self.counter = 0

    def handle_point(self, point_data):
        self.counter += 1
        self.emit({"count": self.counter, self.asfieldname: point_data["data"]["val"] * 2})

    def handle_batch(self, batch_data):
        out_list = list()
        for point in batch_data:
            out_list.append(self.calc(point))
        self.emit(out_list)

    def calc(self, point_dict):
        point_dict[self.asfieldname] = point_dict["data"]["val"] * 2
        return point_dict

    # def __del__(self):
    #     raise Exception("deleted Object!")
