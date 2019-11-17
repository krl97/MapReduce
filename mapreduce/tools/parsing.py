def base_parsing(content: str):
    """ Parse each line in content and split by '-' to get key-value pair """
    lines = content.splitlines()
    return [ tuple(line.split(sep='-')) for line in lines ]