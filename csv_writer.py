import json
import logging
from twarc.decorators2 import FileSizeProgressBar
from more_itertools import ichunked

import dataframe_converter

log = logging.getLogger("twarc")


class CSVConverter:
    """
    JSON Reader and CSV Writer. Converts a given file into CSV, splitting it into chunks, showing progress.
    """

    def __init__(
        self,
        infile,
        outfile,
        converter=dataframe_converter.DataFrameConverter(),
        output_format="csv",
        batch_size=100,
        hide_progress=False,
    ):
        self.infile = infile
        self.outfile = outfile
        self.converter = converter
        self.output_format = output_format
        self.batch_size = batch_size
        self.hide_progress = hide_progress
        self.progress = FileSizeProgressBar(infile, outfile, disable=(hide_progress or not self.infile.seekable()))

    def _read_lines(self):
        """
        Generator for reading files line by line from a file. Progress bar is based on file size.
        """
        line = self.infile.readline()
        while line:
            self.converter.counts["lines"] += 1
            if line.strip() != "":
                try:
                    o = json.loads(line)
                    yield o
                except Exception as ex:
                    self.converter.counts["parse_errors"] += 1
                    log.error(f"Error when trying to parse json: '{line}' {ex}")
            if not self.hide_progress and self.infile.seekable():
                self.progress.update(self.infile.tell() - self.progress.n)
            line = self.infile.readline()

    def _write_output(self, _df, first_batch):
        """
        Write out the dataframe chunk by chunk

        todo: take parameters from commandline for optional output formats.
        """
        if first_batch:
            mode = "w"
            header = True
        else:
            mode = "a+"
            header = False

        self.converter.counts["rows"] += len(_df)
        _df.to_csv(
            self.outfile,
            mode=mode,
            columns=self.converter.output_columns,
            index=False,
            header=header,
        )  # todo: (Optional) arguments for to_csv

    def process(self):
        """
        Process a file containing JSON into a CSV
        """

        # Flag for writing header & appending to CSV file
        first_batch = True
        for batch in ichunked(self._read_lines(), self.batch_size):
            self._write_output(self.converter.process(batch), first_batch)
            first_batch = False

        self.progress.close()
