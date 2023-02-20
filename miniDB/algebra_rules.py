def second_rule(query):

    '''
    rule 2:
    σθ1(σθ2(E)) = σθ2(σθ1(E))
    '''

    newQuery = query

    if 'select' in query.keys() and isinstance(query['from'],dict) and 'select' in query['from'].keys():
        if not (query['distinct'] == False and query['from']['distinct'] == True and query['where'] !=
                query['from']['where']):

            if query['limit'] == None and query['from']['limit'] == None:

                newQuery = query
                newQuery['where'] = newQuery['from']['where']
                newQuery['from']['where'] = query['where']
                return [newQuery]

        return []


def fifth_rule(query):
    '''
    rule 5:
    E1 ⊲⊳θ E2 = E2 ⊲⊳θ E1
    '''
    newQuery = query
    if 'join' in query.keys():
        if query['join'] == 'right':
            newQuery['join'] = 'left'
        elif query['join'] == 'left':
            newQuery['join'] = 'right'
        newQuery['right'] = query['left']
        newQuery['left'] = query['right']
    return newQuery