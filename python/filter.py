from faxe import Faxe


class Filter(Faxe):
    """
    simple batch filter python node
    """

    def handle_point(self, point_data):
        self.emit(point_data)

    def handle_batch(self, batch_data):
        print(batch_data)
        for i, point in enumerate(batch_data['points']):
            if (not point['fields']['field1']) or (point['fields']['field1'] > 20):
                del batch_data['points'][i]

        self.emit(batch_data)
