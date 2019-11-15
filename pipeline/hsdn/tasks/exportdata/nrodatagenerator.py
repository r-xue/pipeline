import os


def get_template():
    data_dir = os.path.dirname(os.path.abspath(__file__))
    data_name = 'scalefile.txt'
    return os.path.join(data_dir, data_name)


def export_template(filename, txt):
    with open(filename, 'w') as f:
        f.write(txt)


def generate(context, datafile):
    tmp = get_template()
    with open(tmp, 'r') as f:
        txt = f.read()
    export_template(datafile, txt)
