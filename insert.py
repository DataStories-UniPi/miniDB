class Insert_stack:
    def __init__(self, row, index_tou_ap_panw, index_ap_katw, table_name):
        self.index_tou_ap_panw = index_tou_ap_panw
        self.index_tou_ap_panw = index_tou_ap_katw
        self.table_name = table_name

        self.create_table('insert_stack',  ['indexa', 'indexb', 'row'], [int, int list])
        self.save()

    def insert(self, row, index_tou_ap_panw, index_ap_katw, table_name):
        old_lst = self._get_insert_stack_for_table(table_name)
        self._update_insert_stack_for_tb(table_name, old_lst+indexes)


    def _get_insert_stack_for_table(self, table_name):
        '''
        Return the insert stack of the specified table
        table_name -> table's name (needs to exist in database)
        '''
        return self.tables['insert_stack']._select_where('*', f'table_name=={table_name}').indexes[0]
        # res = self.select('insert_stack', '*', f'table_name=={table_name}', return_object=True).indexes[0]
        # return res

    def _update_insert_stack(self):
        '''
        updates the insert_stack table
        '''
        for table in self.tables.values():
            if table._name not in self.insert_stack.table_name:
                self.tables['insert_stack']._insert([table._name, []])