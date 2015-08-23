from flask.ext.script import Command, Option
from wikia_dstk.pipeline.data_extraction.run import run as run_data_extraction


class DataExtraction(Command):
    option_list = (
        Option('--workers', dest='workers'),
        Option('--no-shutown', dest='do_shutdown'),
        )

    def run(self):
        run_data_extraction()
