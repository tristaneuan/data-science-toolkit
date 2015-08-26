from flask.ext.script import Command, Option
from wikia_dstk.pipeline.data_extraction.monitor import monitor as monitor_data_extraction


class DataExtraction(Command):
    #option_list = (
    #    Option('--workers', dest='workers'),
    #    Option('--no-shutown', dest='do_shutdown'),
    #    )

    def run(self):
        monitor_data_extraction()
