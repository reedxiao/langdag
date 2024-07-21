class FullSelector():
    '''A selector selects all the idle vertices'''

    def select(self, _, idle):
        '''Select all the idle vertices'''
        return sorted(list(idle), key=lambda x: x.node_id)
    

class MaxSelector():
    """
    A selector selects at most `max_cocurrent` of the idle vertice
    """
    def __init__(self, max_cocurrent):
        self.max_cocurrent = max_cocurrent if max_cocurrent > 1 else 1

    def select(self, running, idle):
        task_number = max(0, self.max_cocurrent-len(running))
        return sorted(list(idle), key=lambda x: x.node_id)[:task_number]
