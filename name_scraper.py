import logging
import string
import tempfile
import unicodedata
from datetime import datetime
from pathlib import Path

import luigi
import pandas as pd

import utils


def move_file(src, dest):
    logger = logging.getLogger(name="luigi-interface")
    logger.info(msg=f"Checking if {dest.absolute()} exists...")
    if dest.exists():
        logger.info(msg=f"Deleting {dest.absolute()}...")
        try:
            dest.unlink(missing_ok=True)
        except FileNotFoundError:
            pass
    logger.info(msg=f"Moving {src.absolute()} to {dest.absolute()}...")
    src.rename(dest)
    logger.info(msg=f"Done moving {src.absolute()} to {dest.absolute()}...")
    return dest


class ScrapeWFTDA(luigi.Task):
    output_dir = luigi.Parameter(default="data")

    def requires(self):
        return []

    def output(self):
        return luigi.LocalTarget(
            path=f"{tempfile.gettempdir()}/wftda-{datetime.now().strftime('%Y%m%d%H%M')}.csv"
        )

    def run(self):
        logger = logging.getLogger(name="luigi-interface")

        logger.info(msg=f"Downloading WFTDA derby names...")
        df = utils.fetch_wftda()
        logger.info(msg=f"Writing {self.output().path}...")
        with self.output().temporary_path() as f:
            df.to_csv(path_or_buf=f, index=False)
        logger.info(msg=f"Done writing {self.output().path}...")
        return move_file(
            src=Path(self.output().path), dest=Path(self.output_dir) / "wftda.csv"
        )


class ScrapeDRC(luigi.Task):
    output_dir = luigi.Parameter(default="data")

    def requires(self):
        return []

    def output(self):
        return luigi.LocalTarget(
            path=f"{tempfile.gettempdir()}/drc-{datetime.now().strftime('%Y%m%d%H%M')}.csv"
        )

    def run(self):
        logger = logging.getLogger(name="luigi-interface")

        logger.info(msg=f"Downloading DRC derby names...")
        df = utils.fetch_drc().sort_values(by=["Name"])
        logger.info(msg=f"Writing {self.output().path}...")
        with self.output().temporary_path() as f:
            df.to_csv(path_or_buf=f, index=False)
        logger.info(msg=f"Done writing {self.output().path}...")
        return move_file(
            src=Path(self.output().path),
            dest=Path(self.output_dir) / "derbyrollcall.csv",
        )


class ScrapeRDR(luigi.Task):
    output_dir = luigi.Parameter(default="data")
    letters = luigi.Parameter(default=string.ascii_uppercase + string.digits)

    def requires(self):
        return []
        # return [ScrapeRDRLetter(letter=letter) for letter in self.letters]

    def output(self):
        return luigi.LocalTarget(
            path=f"{tempfile.gettempdir()}/rdr-{datetime.now().strftime('%Y%m%d%H%M')}.csv"
        )

    def run(self):
        logger = logging.getLogger(name="luigi-interface")

        logger.info(msg=f"Downloading RDR derby names...")
        df = utils.fetch_rdr(letters=self.letters)
        logger.info(msg=f"Writing {self.output().path}...")
        with self.output().temporary_path() as f:
            df.to_csv(path_or_buf=f, index=False)
        logger.info(msg=f"Done writing {self.output().path}...")
        return move_file(
            src=Path(self.output().path),
            dest=Path(self.output_dir) / "rollerderbyroster.csv",
        )


class ScrapeRDN(luigi.Task):
    output_dir = luigi.Parameter(default="data")

    def requires(self):
        return []

    def output(self):
        return luigi.LocalTarget(
            path=f"{tempfile.gettempdir()}/rdn-{datetime.now().strftime('%Y%m%d%H%M')}.csv"
        )

    def run(self):
        logger = logging.getLogger(name="luigi-interface")

        logger.info(msg=f"Downloading RDN derby names...")
        df = utils.fetch_rdn()
        logger.info(msg=f"Writing {self.output().path}...")
        with self.output().temporary_path() as f:
            df.to_csv(path_or_buf=f, index=False)
        logger.info(msg=f"Done writing {self.output().path}...")
        return move_file(
            src=Path(self.output().path), dest=Path(self.output_dir) / "rdnation.csv"
        )


class ScrapeTwoevils(luigi.Task):
    output_dir = luigi.Parameter(default="data")

    def requires(self):
        return []

    def output(self):
        return luigi.LocalTarget(
            path=f"{tempfile.gettempdir()}/twoevils-{datetime.now().strftime('%Y%m%d%H%M')}.csv"
        )

    def run(self):
        logger = logging.getLogger(name="luigi-interface")

        logger.info(msg=f"Downloading Twoevils derby names...")
        df = utils.fetch_twoevils()
        logger.info(msg=f"Writing {self.output().path}...")
        with self.output().temporary_path() as f:
            df.to_csv(path_or_buf=f, index=False)
        logger.info(msg=f"Done writing {self.output().path}...")
        return move_file(
            src=Path(self.output().path), dest=Path(self.output_dir) / "twoevils.csv"
        )


class CombineNames(luigi.Task):
    output_dir = luigi.Parameter(default="data")
    input_csvs = luigi.ListParameter(
        default=[
            "wftda.csv",
            "derbyrollcall.csv",
            "rollerderbyroster.csv",
            "rdnation.csv",
            "twoevils.csv",
        ]
    )
    move_file = luigi.BoolParameter(default=True)

    def requires(self):
        return [
            # ScrapeWFTDA(output_dir=self.output_dir),
            # ScrapeDRC(output_dir=self.output_dir),
            # ScrapeRDR(output_dir=self.output_dir),
            # ScrapeRDN(output_dir=self.output_dir),
            # ScrapeTwoevils(output_dir=self.output_dir),
        ]

    def output(self):
        return luigi.LocalTarget(
            path=f"{tempfile.gettempdir()}/combined-{datetime.now().strftime('%Y%m%d%H%M')}.csv"
        )

    def run(self):
        logger = logging.getLogger(name="luigi-interface")

        logger.info(msg=f"Combining derby names...")
        df = (
            pd.concat(
                objs=[
                    pd.read_csv(
                        filepath_or_buffer=Path(self.output_dir) / csv,
                    )
                    for csv in self.input_csvs
                ],
                ignore_index=True,
            )
            .drop_duplicates(subset=["Name"])
            .sort_values(by=["Name", "Number"])
        )
        logger.info(msg=f"Writing {self.output().path}...")
        with self.output().temporary_path() as f:
            df.to_csv(path_or_buf=f, index=False)
        logger.info(msg=f"Done writing {self.output().path}...")
        if self.move_file:
            return move_file(
                src=Path(self.output().path),
                dest=Path(self.output_dir) / "derby_names.csv",
            )
        else:
            return self.output()


class NameList(luigi.Task):
    output_dir = luigi.Parameter(default="data")
    ascii_only = luigi.BoolParameter(default=False)

    def requires(self):
        return {"names": CombineNames(output_dir=self.output_dir, move_file=False)}

    def output(self):
        return luigi.LocalTarget(
            path=f"{tempfile.gettempdir()}/namelist-{datetime.now().strftime('%Y%m%d%H%M')}.csv"
        )

    def run(self):
        logger = logging.getLogger(name="luigi-interface")

        logger.info(msg=f"Creating name list...")
        logger.debug(msg=self.input()["names"])
        df = pd.read_csv(filepath_or_buffer=self.input()["names"].path)
        df = df.drop_duplicates(subset=["Name"])[["Name"]]
        logger.debug(df.columns)
        if self.ascii_only:
            df["Name"] = df["Name"].apply(
                lambda x: unicodedata.normalize("NFKD", x)
                .encode(encoding="ascii", errors="ignore")
                .decode(encoding="utf-8")
            )
            outfile = "derby_names_ascii.txt"
        else:
            outfile = "derby_names.txt"
        df = df.sort_values(by=["Name"])
        logger.info(msg=f"Writing {self.output().path}...")
        with self.output().temporary_path() as f:
            df.to_csv(path_or_buf=f, index=False, header=False)
        logger.info(msg=f"Done writing {self.output().path}...")
        return move_file(
            src=Path(self.output().path), dest=Path(self.output_dir) / outfile
        )


class NumberList(luigi.Task):
    output_dir = luigi.Parameter(default="data")

    def requires(self):
        return {"names": CombineNames(output_dir=self.output_dir, move_file=False)}

    def output(self):
        return luigi.LocalTarget(
            path=f"{tempfile.gettempdir()}/numberlist-{datetime.now().strftime('%Y%m%d%H%M')}.csv"
        )

    def run(self):
        logger = logging.getLogger(name="luigi-interface")

        logger.info(msg=f"Creating number list...")
        logger.debug(msg=self.input()["names"])
        df = pd.read_csv(filepath_or_buffer=self.input()["names"].path)
        df = df.drop_duplicates(subset=["Number"]).sort_values(by=["Number"])["Number"]
        logger.info(msg=f"Writing {self.output().path}...")
        with self.output().temporary_path() as f:
            df.to_csv(path_or_buf=f, index=False, header=False)
        logger.info(msg=f"Done writing {self.output().path}...")
        return move_file(
            src=Path(self.output().path),
            dest=Path(self.output_dir) / "derby_numbers.txt",
        )


class NameNumberList(luigi.Task):
    output_dir = luigi.Parameter(default="data")

    def requires(self):
        return {"names": CombineNames(output_dir=self.output_dir, move_file=False)}

    def output(self):
        return luigi.LocalTarget(
            path=f"{tempfile.gettempdir()}/namenumberlist-{datetime.now().strftime('%Y%m%d%H%M')}.csv"
        )

    def run(self):
        logger = logging.getLogger(name="luigi-interface")

        logger.info(msg=f"Creating name and number list...")
        logger.debug(msg=self.input()["names"])
        name_df = pd.read_csv(filepath_or_buffer=self.input()["names"].path)
        names_numbers = (
            name_df[~name_df["Number"].isna()][["Name", "Number"]]
            .drop_duplicates()
            .sort_values(by=["Name", "Number"])
        )
        logger.info(msg=f"Writing {self.output().path}...")
        with self.output().temporary_path() as f:
            names_numbers.to_csv(path_or_buf=f, index=False, header=False, sep="\t")
        logger.info(msg=f"Done writing {self.output().path}...")
        return move_file(
            src=Path(self.output().path),
            dest=Path(self.output_dir) / "derby_names_numbers.tsv",
        )


if __name__ == "__main__":
    luigi.run()
