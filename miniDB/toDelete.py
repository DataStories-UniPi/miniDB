for i in range(global_depth + 1):
    # if this bucket  is not the one to be split it should be saved as it is
    if i < pow(2, global_depth - 1) and i != int(position):
        new_dict[format(i, 'b')] = dict[format(i, 'b')]

    # the bucket being split
    else:

else:
    # get the elements from the overflowed bucket + the new element
    elements = dict[position].values
    elements += [elem]
    # position the elements according to the hash function
    for item in elements:
        print('elements are ', elements)
        # Convert to binary
        b = get_binary(item)
        # Hash function => result mod 2 ^ global depth
        lsb = int(b) % pow(2, global_depth)
        insert(new_dict, format(lsb, 'b'), item, global_depth)