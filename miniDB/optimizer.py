
class Optimizer:
    def __init__(self, query):
        self.input_query_list = query.split()
        self.kw_ops = set(['and', 'or'])
        
    '''
        Swaps condition tokens for possible index usage, example:
        for the condition: id between 30 and 50 or name = hello
        suppose there is a supported index on column 'name' but not on 'id'
        it would be better if we swapped the tokens as such:
        name = hello or id between 30 and 50
        this way we would make use of the index on column name resulting in faster execution time
    '''    
    def swap_condition_tokens_of_where(self):
        for kw_op in self.kw_ops:
            try:
                kw_op_idx = self.input_query_list.index(kw_op)
                if self.input_query_list[kw_op_idx - 2] == 'between':
                    l_sub_list = self.input_query_list[:kw_op_idx + 2]
                    r_sub_list = self.input_query_list[kw_op_idx + 3:]
                    alt_query = ' '.join(r_sub_list) + ' ' + self.input_query_list[kw_op_idx + 2] + ' ' + ' '.join(l_sub_list)
                else:
                    l_sub_list = self.input_query_list[:kw_op_idx]
                    r_sub_list = self.input_query_list[kw_op_idx + 1:]
                    alt_query = ' '.join(r_sub_list) + ' ' + kw_op + ' ' + ' '.join(l_sub_list)
                return alt_query
            except ValueError:
                pass        
    
    def gen_query_alts(self):
        alts = []
        alts.append(self.swap_condition_tokens_of_where())
        return alts
                
                

opt = Optimizer('id between 30 and 50 or name = hello')
print(opt.gen_query_alts())
