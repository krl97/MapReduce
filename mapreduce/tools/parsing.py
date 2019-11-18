def base_parsing(lines):
    """ Parse each line in content and split by '-' to get key-value pair """
    return [ tuple(line.split(sep='-')) for line in lines ]