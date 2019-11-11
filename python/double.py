from faxe import Faxe


class Double(Faxe):

    @staticmethod
    def options():
        opts = [
            (b"field", b"string"),
            (b"as", b"string")
        ]
        return opts

    def init(self, args):
        self.fieldname = args[b'field']
        self.asfieldname = args[b'as']
        self.points = list()
        print("my args: ", args)

    def handle_point(self, point_data):
        print(self.points)
        print("point ", point_data)
        self.points.append(point_data)
        if len(self.points) > 99:
            return self.emit(self.calc(point_data))
        return

    def handle_batch(self, batch_data):
        out_list = list()
        for point in batch_data:
            out_list.append(self.calc(point))
        return self.emit(out_list)

    def calc(self, point_dict):
        point_dict[self.asfieldname] = point_dict[self.fieldname] * 2
        return point_dict
