def read_ini(data):
    dict_data = {}
    for line in data:
        k = line.split("=")[0]
        v = line.split("=")[1]
        k = k.replace("\n", "")
        v = v.replace("\n", "")
        dict_data[k] = v
    return dict_data