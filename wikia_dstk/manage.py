from flask.ext.script import Manager
from app import app
from wikia_dstk.commands import data_extraction

manager = Manager(app)

scripts = {
    'data_extraction': data_extraction.DataExtraction(),
    }

if __name__ == '__main__':
    manager.run(scripts)
