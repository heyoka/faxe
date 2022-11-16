from faxe import Faxe, Point, Batch


class Data_demo(Faxe):
    """
    simple noop faxe python node callback
    """

    def handle_point(self, point_data):

        # just add a field and emit the data-point
        self.log("point field ", Point.value(point_data, 'pyth.field3.deeper.list[0]'))
        Point.value(point_data,'glippse.globbsy.p_ts[0]', Faxe.now())
        self.log('result after setting data', point_data)

        self.emit(point_data)

    def handle_batch(self, batch_data):
        # HELPER CLASSES DEMO ##########
        """
        ############################################################################################
        completely ignoring the input data_batch, we build our own and emit it ;)
        
        here we can see, that the helper classes are completely optional to use
        we can always directly work on the datastructure
        
        the helper classes work directly on the data structure, and they do not hold an internal representation of the 
        data structure themselves
        
        :param batch_data:
        :return: void
        """
        newpoint = Point.new(1)
        Point.field(newpoint, 'python_field', 1234)

        newpoint2 = Point.new(2)
        Point.field(newpoint2, 'python_field2', 2345)

        newpoint3 = Point.new(Faxe.now())
        point3fields = \
            {'python_field3': 5678, 'python_field3.1': 12.546, 'python_field3.2': {'sub': ['alle', 'sind', 'da%$<-']}}
        Point.fields(newpoint3, point3fields)

        newpoint4 = dict()
        newpoint4['fields'] = {'python_field3': 6464}
        newpoint4['ts'] = Faxe.now()+10

        lastpoint = Point.new(7)
        Point.field(lastpoint, 'python_field_7', 8773)

        # create batch and add all the points to it
        batch_out = Batch.new(9876)
        Batch.points(batch_out, [newpoint2, lastpoint, newpoint3, newpoint, newpoint4])

        self.log('output ' + str(batch_out), 'info')
        self.emit(batch_out)
