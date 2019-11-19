def base_parsing(lines):
    """ Parse each line in content and split by '-' to get key-value pair """
    lines = [l.strip() for l in lines]
    return [ tuple(line.split(sep='-')) for line in lines ]