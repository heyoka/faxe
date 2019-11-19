from faxe import Faxe


class Double(Faxe):

    @staticmethod
    def options():
        opts = [
            (b'field', b'string'),
            (b'as', b'string')
        ]
        return opts

    def init(self, args):
        self.fieldname = args[b'field']
        self.asfieldname = args[b'as']
        print("my args: ", args)

    def handle_point(self, point_data):
        self.emit(self.calc(point_data))

    def handle_batch(self, batch_data):
        out_list = list()
        for point in batch_data:
            out_list.append(self.calc(point))
        self.emit(out_list)

    def calc(self, point_dict):
        point_dict[self.asfieldname] = point_dict[self.fieldname] * 2
        return point_dict
